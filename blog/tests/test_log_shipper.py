"""
test_log_shipper.py — Unit tests for the LogShipper class.

Validates:
- HTTP POST is sent to the correct OpenSearch endpoint
- SigV4 Authorization header is present (contains 'AWS4-HMAC-SHA256')
- No Basic Authorization header is sent
- Retry logic: succeeds on second attempt after one failure
- Raises LogShipperError after 4 failed attempts
- Exponential backoff delays: 1 s, 2 s, 4 s

Requirements: 4.2, 4.3
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest
import requests

from blog.code.log_event import LogEvent
from blog.code.log_shipper import LogShipper, LogShipperError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ENDPOINT = "https://search-test-domain-abc123.us-east-1.es.amazonaws.com"
_INDEX = "etl-logs-2024-03"
_REGION = "us-east-1"

_SAMPLE_EVENT = LogEvent(
    run_id="scheduled__2024-03-15T10:00:00+00:00",
    task_name="glue_extraction",
    component_type="glue",
    log_level="INFO",
    message="Glue job completed successfully",
    timestamp="2024-03-15T10:28:45Z",
)

# Fake AWS credentials used in all tests so boto3 never hits real AWS
_FAKE_CREDS = {
    "access_key": "AKIAIOSFODNN7EXAMPLE",
    "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "token": None,
}


def _make_frozen_creds():
    """Return a mock frozen credentials object."""
    creds = MagicMock()
    creds.access_key = _FAKE_CREDS["access_key"]
    creds.secret_key = _FAKE_CREDS["secret_key"]
    creds.token = _FAKE_CREDS["token"]
    return creds


def _make_shipper() -> LogShipper:
    """Construct a LogShipper with mocked boto3 credentials."""
    with patch("blog.code.log_shipper.boto3.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_creds = MagicMock()
        mock_creds.get_frozen_credentials.return_value = _make_frozen_creds()
        mock_session.get_credentials.return_value = mock_creds
        return LogShipper(_ENDPOINT, _INDEX, _REGION)


def _ok_response() -> MagicMock:
    """Return a mock HTTP 200 response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 200
    resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# Tests: HTTP request shape
# ---------------------------------------------------------------------------


class TestShipRequestShape:
    """Verify the HTTP request sent by ship() has the correct shape."""

    def test_posts_to_correct_url(self):
        """ship() sends a POST to <endpoint>/<index>/_doc."""
        shipper = _make_shipper()
        with patch("blog.code.log_shipper.requests.post") as mock_post:
            mock_post.return_value = _ok_response()
            shipper.ship(_SAMPLE_EVENT)

        mock_post.assert_called_once()
        url_arg = mock_post.call_args[0][0]
        assert url_arg == f"{_ENDPOINT}/{_INDEX}/_doc"

    def test_payload_is_valid_json(self):
        """ship() sends the LogEvent serialized as valid JSON."""
        shipper = _make_shipper()
        with patch("blog.code.log_shipper.requests.post") as mock_post:
            mock_post.return_value = _ok_response()
            shipper.ship(_SAMPLE_EVENT)

        kwargs = mock_post.call_args[1]
        payload = kwargs["data"]
        parsed = json.loads(payload)
        assert parsed["run_id"] == _SAMPLE_EVENT.run_id
        assert parsed["task_name"] == _SAMPLE_EVENT.task_name

    def test_content_type_header_is_json(self):
        """ship() sets Content-Type: application/json."""
        shipper = _make_shipper()
        with patch("blog.code.log_shipper.requests.post") as mock_post:
            mock_post.return_value = _ok_response()
            shipper.ship(_SAMPLE_EVENT)

        kwargs = mock_post.call_args[1]
        assert kwargs["headers"]["Content-Type"] == "application/json"

    def test_sigv4_auth_header_present(self):
        """ship() includes a SigV4 Authorization header (AWS4-HMAC-SHA256)."""
        shipper = _make_shipper()
        captured_requests: list[requests.PreparedRequest] = []

        def fake_post(url, **kwargs):
            # Build a PreparedRequest so we can inspect the Authorization header
            # that AWS4Auth would add
            auth = kwargs.get("auth")
            req = requests.Request("POST", url, headers=kwargs.get("headers", {}))
            prepared = req.prepare()
            if auth:
                prepared = auth(prepared)
            captured_requests.append(prepared)
            return _ok_response()

        with patch("blog.code.log_shipper.requests.post", side_effect=fake_post):
            shipper.ship(_SAMPLE_EVENT)

        assert len(captured_requests) == 1
        auth_header = captured_requests[0].headers.get("Authorization", "")
        assert "AWS4-HMAC-SHA256" in auth_header, (
            f"Expected SigV4 Authorization header, got: {auth_header!r}"
        )

    def test_no_basic_auth_header(self):
        """ship() must not include an Authorization: Basic header."""
        shipper = _make_shipper()
        captured_requests: list[requests.PreparedRequest] = []

        def fake_post(url, **kwargs):
            auth = kwargs.get("auth")
            req = requests.Request("POST", url, headers=kwargs.get("headers", {}))
            prepared = req.prepare()
            if auth:
                prepared = auth(prepared)
            captured_requests.append(prepared)
            return _ok_response()

        with patch("blog.code.log_shipper.requests.post", side_effect=fake_post):
            shipper.ship(_SAMPLE_EVENT)

        assert len(captured_requests) == 1
        auth_header = captured_requests[0].headers.get("Authorization", "")
        assert "Basic" not in auth_header, (
            f"Unexpected Basic auth header: {auth_header!r}"
        )


# ---------------------------------------------------------------------------
# Tests: Retry logic
# ---------------------------------------------------------------------------


class TestRetryLogic:
    """Verify retry behaviour and exponential backoff."""

    def test_succeeds_on_second_attempt_after_one_failure(self):
        """ship() retries and succeeds when the first attempt raises an exception."""
        shipper = _make_shipper()
        side_effects = [
            requests.exceptions.ConnectionError("connection refused"),
            _ok_response(),
        ]

        with patch("blog.code.log_shipper.requests.post", side_effect=side_effects) as mock_post:
            with patch("blog.code.log_shipper.time.sleep"):
                shipper.ship(_SAMPLE_EVENT)  # should not raise

        assert mock_post.call_count == 2

    def test_raises_log_shipper_error_after_four_failures(self):
        """ship() raises LogShipperError after exactly 4 failed attempts."""
        shipper = _make_shipper()
        side_effects = [
            requests.exceptions.ConnectionError("connection refused")
        ] * 4

        with patch("blog.code.log_shipper.requests.post", side_effect=side_effects) as mock_post:
            with patch("blog.code.log_shipper.time.sleep"):
                with pytest.raises(LogShipperError):
                    shipper.ship(_SAMPLE_EVENT)

        assert mock_post.call_count == 4

    def test_exactly_four_total_attempts(self):
        """ship() makes exactly 4 total attempts (1 initial + 3 retries)."""
        shipper = _make_shipper()
        side_effects = [
            requests.exceptions.Timeout("timed out")
        ] * 4

        with patch("blog.code.log_shipper.requests.post", side_effect=side_effects) as mock_post:
            with patch("blog.code.log_shipper.time.sleep"):
                with pytest.raises(LogShipperError):
                    shipper.ship(_SAMPLE_EVENT)

        assert mock_post.call_count == 4

    def test_exponential_backoff_delays(self):
        """ship() sleeps for 1 s, 2 s, 4 s between attempts."""
        shipper = _make_shipper()
        side_effects = [
            requests.exceptions.ConnectionError("connection refused")
        ] * 4

        with patch("blog.code.log_shipper.requests.post", side_effect=side_effects):
            with patch("blog.code.log_shipper.time.sleep") as mock_sleep:
                with pytest.raises(LogShipperError):
                    shipper.ship(_SAMPLE_EVENT)

        # Three sleeps between four attempts
        assert mock_sleep.call_count == 3
        assert mock_sleep.call_args_list == [call(1), call(2), call(4)]

    def test_no_sleep_on_success(self):
        """ship() does not sleep when the first attempt succeeds."""
        shipper = _make_shipper()

        with patch("blog.code.log_shipper.requests.post", return_value=_ok_response()):
            with patch("blog.code.log_shipper.time.sleep") as mock_sleep:
                shipper.ship(_SAMPLE_EVENT)

        mock_sleep.assert_not_called()

    def test_http_error_triggers_retry(self):
        """ship() retries on HTTP error responses (e.g. 503)."""
        shipper = _make_shipper()

        error_response = MagicMock(spec=requests.Response)
        error_response.status_code = 503
        error_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "503 Service Unavailable"
        )

        side_effects = [error_response, _ok_response()]

        with patch("blog.code.log_shipper.requests.post", side_effect=side_effects) as mock_post:
            with patch("blog.code.log_shipper.time.sleep"):
                shipper.ship(_SAMPLE_EVENT)

        assert mock_post.call_count == 2


# ---------------------------------------------------------------------------
# Tests: Secrets Manager integration
# ---------------------------------------------------------------------------


class TestGetEndpointFromSecretsManager:
    """Verify _get_endpoint_from_secrets_manager() retrieves the endpoint correctly."""

    def test_retrieves_endpoint_from_json_secret(self):
        """Returns the 'opensearch_endpoint' value from a JSON secret."""
        secret_value = json.dumps(
            {"opensearch_endpoint": "https://search-my-domain.us-east-1.es.amazonaws.com"}
        )
        with patch("blog.code.log_shipper.boto3.client") as mock_client_fn:
            mock_client = MagicMock()
            mock_client_fn.return_value = mock_client
            mock_client.get_secret_value.return_value = {"SecretString": secret_value}

            result = LogShipper._get_endpoint_from_secrets_manager("my-secret")

        assert result == "https://search-my-domain.us-east-1.es.amazonaws.com"

    def test_retrieves_endpoint_from_plain_string_secret(self):
        """Returns the raw string when the secret is not JSON."""
        plain_endpoint = "https://search-my-domain.us-east-1.es.amazonaws.com"
        with patch("blog.code.log_shipper.boto3.client") as mock_client_fn:
            mock_client = MagicMock()
            mock_client_fn.return_value = mock_client
            mock_client.get_secret_value.return_value = {"SecretString": plain_endpoint}

            result = LogShipper._get_endpoint_from_secrets_manager("my-secret")

        assert result == plain_endpoint

    def test_raises_key_error_when_json_missing_key(self):
        """Raises KeyError when JSON secret lacks 'opensearch_endpoint' key."""
        secret_value = json.dumps({"some_other_key": "value"})
        with patch("blog.code.log_shipper.boto3.client") as mock_client_fn:
            mock_client = MagicMock()
            mock_client_fn.return_value = mock_client
            mock_client.get_secret_value.return_value = {"SecretString": secret_value}

            with pytest.raises(KeyError):
                LogShipper._get_endpoint_from_secrets_manager("my-secret")
