"""
log_shipper.py — LogShipper class for the MWAA + OpenSearch monitoring blog.

Sends ``LogEvent`` documents to an Amazon OpenSearch Service index using
IAM/SigV4 authentication (no username/password credentials).  Includes
retry logic with exponential backoff and optional endpoint retrieval from
AWS Secrets Manager.

Compatible with Python 3.9+.

Dependencies
------------
- boto3>=1.34.0
- requests>=2.31.0
- requests-aws4auth==1.3.1
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Optional

import boto3
import requests
from requests_aws4auth import AWS4Auth

from blog.code.log_event import LogEvent

# ---------------------------------------------------------------------------
# Module-level logger — errors are emitted to stderr
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.setLevel(logging.ERROR)
logger.addHandler(_stderr_handler)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_ATTEMPTS = 4          # 1 initial attempt + 3 retries
_BACKOFF_DELAYS = (1, 2, 4)  # seconds between attempts 1→2, 2→3, 3→4
_SERVICE = "es"            # AWS service name for SigV4 signing


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class LogShipperError(Exception):
    """Raised when all retry attempts to ship a ``LogEvent`` have been exhausted."""


# ---------------------------------------------------------------------------
# LogShipper
# ---------------------------------------------------------------------------


class LogShipper:
    """Ships ``LogEvent`` documents to Amazon OpenSearch Service via SigV4.

    Parameters
    ----------
    opensearch_endpoint : str
        The HTTPS endpoint of the OpenSearch domain, e.g.
        ``"https://search-my-domain-abc123.us-east-1.es.amazonaws.com"``.
        Do **not** include a trailing slash.
    index_name : str
        The name of the OpenSearch index to write to, e.g. ``"etl-logs-2024-03"``.
    region : str
        The AWS region where the OpenSearch domain is deployed, e.g. ``"us-east-1"``.

    Notes
    -----
    - Credentials are obtained automatically from the boto3 credential chain
      (IAM role, environment variables, ``~/.aws/credentials``).  No
      username/password credentials are used.
    - The ``ship()`` method retries up to 3 times (4 total attempts) with
      exponential backoff delays of 1 s, 2 s, 4 s.  On final failure it
      raises ``LogShipperError`` and emits an error log to stderr.
    """

    def __init__(
        self,
        opensearch_endpoint: str,
        index_name: str,
        region: str,
    ) -> None:
        self._endpoint = opensearch_endpoint.rstrip("/")
        self._index_name = index_name
        self._region = region
        self._auth = self._build_auth()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ship(self, event: LogEvent) -> None:
        """Serialize *event* and POST it to the OpenSearch index.

        Retries up to 3 times (4 total attempts) with exponential backoff
        delays of 1 s, 2 s, 4 s.  On final failure raises ``LogShipperError``
        and emits an error message to stderr.

        Parameters
        ----------
        event : LogEvent
            The structured log event to ship.

        Raises
        ------
        LogShipperError
            When all retry attempts have been exhausted.
        """
        url = f"{self._endpoint}/{self._index_name}/_doc"
        payload = json.dumps(event.to_dict())
        headers = {"Content-Type": "application/json"}

        last_exc: Optional[Exception] = None

        for attempt in range(_MAX_ATTEMPTS):
            try:
                response = requests.post(
                    url,
                    data=payload,
                    headers=headers,
                    auth=self._auth,
                    timeout=10,
                )
                response.raise_for_status()
                return  # success — exit immediately
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                if attempt < _MAX_ATTEMPTS - 1:
                    delay = _BACKOFF_DELAYS[attempt]
                    logger.debug(
                        "LogShipper: attempt %d/%d failed (%s); retrying in %ds",
                        attempt + 1,
                        _MAX_ATTEMPTS,
                        exc,
                        delay,
                    )
                    time.sleep(delay)

        # All attempts exhausted
        error_msg = (
            f"LogShipper: failed to ship event after {_MAX_ATTEMPTS} attempts. "
            f"run_id={event.run_id!r}, task_name={event.task_name!r}. "
            f"Last error: {last_exc}"
        )
        logger.error(error_msg)
        raise LogShipperError(error_msg) from last_exc

    # ------------------------------------------------------------------
    # Class methods
    # ------------------------------------------------------------------

    @classmethod
    def _get_endpoint_from_secrets_manager(cls, secret_name: str) -> str:
        """Retrieve the OpenSearch endpoint from AWS Secrets Manager.

        The secret value must be a JSON object with an ``"opensearch_endpoint"``
        key, or a plain string containing the endpoint URL.

        Parameters
        ----------
        secret_name : str
            The name or ARN of the Secrets Manager secret.

        Returns
        -------
        str
            The OpenSearch endpoint URL.

        Raises
        ------
        KeyError
            If the secret is a JSON object but does not contain the
            ``"opensearch_endpoint"`` key.
        ValueError
            If the secret value cannot be parsed.
        """
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response["SecretString"]

        # Try to parse as JSON first; fall back to treating as a plain string
        try:
            secret_data = json.loads(secret_string)
            if isinstance(secret_data, dict):
                return secret_data["opensearch_endpoint"]
            # JSON scalar (e.g. a quoted string)
            return str(secret_data)
        except (json.JSONDecodeError, TypeError):
            # Plain string endpoint
            return secret_string

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_auth(self) -> AWS4Auth:
        """Build an ``AWS4Auth`` instance using the current boto3 credentials."""
        session = boto3.Session()
        credentials = session.get_credentials()
        # Resolve any lazy/refreshable credentials to their concrete values
        resolved = credentials.get_frozen_credentials()
        return AWS4Auth(
            resolved.access_key,
            resolved.secret_key,
            self._region,
            _SERVICE,
            session_token=resolved.token,
        )
