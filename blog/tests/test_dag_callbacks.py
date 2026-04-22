"""
test_dag_callbacks.py — Unit tests for the MWAA DAG callbacks.

Tests cover:
- ``notify_failure``: publishes to SNS with the correct message structure
  (``run_id``, ``task_name``, ``failure_reason``, ``timestamp``) and ships a
  ``Log_Event`` to OpenSearch.
- ``write_dag_summary``: POSTs a summary document to OpenSearch containing
  ``run_id``, ``overall_status``, ``total_duration`` (``duration_ms``), and
  ``task_statuses``.
- Both callbacks swallow ``LogShipper`` / SNS exceptions rather than re-raising
  them, so a monitoring failure never masks the original pipeline outcome.

Requirements: 6.2, 6.3
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_task_instance(task_id: str = "glue_extraction") -> MagicMock:
    ti = MagicMock()
    ti.task_id = task_id
    ti.state = "failed"
    return ti


def _make_dag_run(
    start_date: datetime | None = None,
    task_ids: list[str] | None = None,
) -> MagicMock:
    dag_run = MagicMock()
    dag_run.start_date = start_date or datetime(2024, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
    task_ids = task_ids or ["glue_extraction", "lambda_transform", "ec2_custom_script"]
    tis = []
    for tid in task_ids:
        ti = MagicMock()
        ti.task_id = tid
        ti.state = "success"
        tis.append(ti)
    dag_run.get_task_instances.return_value = tis
    return dag_run


def _make_failure_context(
    run_id: str = "scheduled__2024-03-15T10:00:00+00:00",
    task_id: str = "glue_extraction",
    exception: Exception | None = None,
) -> dict:
    return {
        "run_id": run_id,
        "task_instance": _make_task_instance(task_id),
        "exception": exception or ValueError("Glue job failed"),
    }


def _make_success_context(
    run_id: str = "scheduled__2024-03-15T10:00:00+00:00",
    task_ids: list[str] | None = None,
) -> dict:
    return {
        "run_id": run_id,
        "dag_run": _make_dag_run(task_ids=task_ids),
        "dag": MagicMock(),
    }


@staticmethod
def _variable_get_side_effect(key: str, default_var: str = "") -> str:
    mapping = {
        "sns_failure_topic_arn": "arn:aws:sns:us-east-1:123456789012:etl-failures",
        "opensearch_secret_name": "etl/opensearch/endpoint",
        "opensearch_index_name": "etl-logs",
        "aws_region": "us-east-1",
    }
    return mapping.get(key, default_var)


# ---------------------------------------------------------------------------
# Import guard — verify the callbacks module imports without errors
# ---------------------------------------------------------------------------


class TestImport:
    def test_dag_callbacks_module_imports_without_error(self) -> None:
        """The dag_callbacks module must be importable without raising any exception."""
        with (
            patch("airflow.models.Variable.get", return_value="dummy"),
            patch("boto3.client"),
            patch("boto3.Session"),
        ):
            import blog.code.dag_callbacks  # noqa: F401

    def test_notify_failure_is_callable(self) -> None:
        from blog.code.dag_callbacks import notify_failure

        assert callable(notify_failure)

    def test_write_dag_summary_is_callable(self) -> None:
        from blog.code.dag_callbacks import write_dag_summary

        assert callable(write_dag_summary)


# ---------------------------------------------------------------------------
# notify_failure tests
# ---------------------------------------------------------------------------


class TestNotifyFailure:
    """Tests for the ``notify_failure`` on_failure_callback."""

    def test_sns_publish_called_with_required_fields(self) -> None:
        """notify_failure must publish to SNS with run_id, task_name, failure_reason, timestamp."""
        from blog.code.dag_callbacks import notify_failure

        sns_mock = MagicMock()
        shipper_mock = MagicMock()
        context = _make_failure_context(
            run_id="run-abc-123",
            task_id="glue_extraction",
            exception=RuntimeError("Glue job timed out"),
        )

        with (
            patch("blog.code.dag_callbacks.Variable.get", side_effect=_variable_get_side_effect),
            patch("blog.code.dag_callbacks.boto3.client", return_value=sns_mock),
            patch("blog.code.dag_callbacks._get_log_shipper", return_value=shipper_mock),
        ):
            notify_failure(context)

        sns_mock.publish.assert_called_once()
        call_kwargs = sns_mock.publish.call_args[1]

        assert call_kwargs["TopicArn"] == "arn:aws:sns:us-east-1:123456789012:etl-failures"
        assert "ETL Pipeline Failure" in call_kwargs["Subject"]

        message = json.loads(call_kwargs["Message"])
        assert message["run_id"] == "run-abc-123"
        assert message["task_name"] == "glue_extraction"
        assert "Glue job timed out" in message["failure_reason"]
        assert "timestamp" in message
        assert isinstance(message["timestamp"], str) and message["timestamp"]

    def test_sns_publish_message_has_all_four_required_fields(self) -> None:
        """The SNS message JSON must contain exactly the four required fields."""
        from blog.code.dag_callbacks import notify_failure

        sns_mock = MagicMock()
        shipper_mock = MagicMock()
        context = _make_failure_context()

        with (
            patch("blog.code.dag_callbacks.Variable.get", side_effect=_variable_get_side_effect),
            patch("blog.code.dag_callbacks.boto3.client", return_value=sns_mock),
            patch("blog.code.dag_callbacks._get_log_shipper", return_value=shipper_mock),
        ):
            notify_failure(context)

        message = json.loads(sns_mock.publish.call_args[1]["Message"])
        for field in ("run_id", "task_name", "failure_reason", "timestamp"):
            assert field in message, f"Missing required SNS field: {field}"

    def test_log_shipper_ship_called_with_mwaa_component_type(self) -> None:
        """notify_failure must ship a Log_Event with component_type='mwaa' and log_level='ERROR'."""
        from blog.code.dag_callbacks import notify_failure

        sns_mock = MagicMock()
        shipper_mock = MagicMock()
        context = _make_failure_context(run_id="run-xyz", task_id="lambda_transform")

        with (
            patch("blog.code.dag_callbacks.Variable.get", side_effect=_variable_get_side_effect),
            patch("blog.code.dag_callbacks.boto3.client", return_value=sns_mock),
            patch("blog.code.dag_callbacks._get_log_shipper", return_value=shipper_mock),
        ):
            notify_failure(context)

        shipper_mock.ship.assert_called_once()
        shipped_event = shipper_mock.ship.call_args[0][0]
        assert shipped_event.component_type == "mwaa"
        assert shipped_event.log_level == "ERROR"
        assert shipped_event.run_id == "run-xyz"
        assert shipped_event.task_name == "lambda_transform"

    def test_sns_failure_does_not_raise(self) -> None:
        """If SNS publish raises, notify_failure must swallow the error."""
        from blog.code.dag_callbacks import notify_failure

        sns_mock = MagicMock()
        sns_mock.publish.side_effect = Exception("SNS unavailable")
        shipper_mock = MagicMock()
        context = _make_failure_context()

        with (
            patch("blog.code.dag_callbacks.Variable.get", side_effect=_variable_get_side_effect),
            patch("blog.code.dag_callbacks.boto3.client", return_value=sns_mock),
            patch("blog.code.dag_callbacks._get_log_shipper", return_value=shipper_mock),
        ):
            # Should not raise
            notify_failure(context)

    def test_log_shipper_failure_does_not_raise(self) -> None:
        """If LogShipper.ship raises, notify_failure must swallow the error."""
        from blog.code.dag_callbacks import notify_failure

        sns_mock = MagicMock()
        shipper_mock = MagicMock()
        shipper_mock.ship.side_effect = Exception("OpenSearch unreachable")
        context = _make_failure_context()

        with (
            patch("blog.code.dag_callbacks.Variable.get", side_effect=_variable_get_side_effect),
            patch("blog.code.dag_callbacks.boto3.client", return_value=sns_mock),
            patch("blog.code.dag_callbacks._get_log_shipper", return_value=shipper_mock),
        ):
            # Should not raise
            notify_failure(context)

    def test_get_log_shipper_failure_does_not_raise(self) -> None:
        """If _get_log_shipper raises, notify_failure must swallow the error."""
        from blog.code.dag_callbacks import notify_failure

        sns_mock = MagicMock()
        context = _make_failure_context()

        with (
            patch("blog.code.dag_callbacks.Variable.get", side_effect=_variable_get_side_effect),
            patch("blog.code.dag_callbacks.boto3.client", return_value=sns_mock),
            patch(
                "blog.code.dag_callbacks._get_log_shipper",
                side_effect=Exception("Secrets Manager unavailable"),
            ),
        ):
            # Should not raise
            notify_failure(context)


# ---------------------------------------------------------------------------
# write_dag_summary tests
# ---------------------------------------------------------------------------


class TestWriteDagSummary:
    """Tests for the ``write_dag_summary`` on_success_callback."""

    def test_summary_event_contains_required_fields(self) -> None:
        """write_dag_summary must POST a document with run_id, overall_status,
        total_duration (duration_ms), and task_statuses."""
        from blog.code.dag_callbacks import write_dag_summary

        response_mock = MagicMock()
        response_mock.raise_for_status.return_value = None
        shipper_mock = MagicMock()
        shipper_mock._endpoint = "https://search-test.us-east-1.es.amazonaws.com"
        shipper_mock._index_name = "etl-logs"
        shipper_mock._auth = MagicMock()

        context = _make_success_context(
            run_id="run-summary-001",
            task_ids=["glue_extraction", "lambda_transform", "ec2_custom_script"],
        )

        posted_bodies: list[dict] = []

        def capture_post(url, data, **kwargs):
            posted_bodies.append(json.loads(data))
            return response_mock

        with (
            patch("blog.code.dag_callbacks.Variable.get", side_effect=_variable_get_side_effect),
            patch("blog.code.dag_callbacks.boto3.client"),
            patch("blog.code.dag_callbacks._get_log_shipper", return_value=shipper_mock),
            patch("blog.code.dag_callbacks.requests.post", side_effect=capture_post),
        ):
            write_dag_summary(context)

        assert len(posted_bodies) == 1, "Expected exactly one POST to OpenSearch"
        doc = posted_bodies[0]

        assert doc["run_id"] == "run-summary-001"
        assert doc["overall_status"] == "success"
        assert "duration_ms" in doc
        assert isinstance(doc["duration_ms"], int)
        assert doc["duration_ms"] >= 0
        assert "task_statuses" in doc
        assert isinstance(doc["task_statuses"], dict)
        for tid in ("glue_extraction", "lambda_transform", "ec2_custom_script"):
            assert tid in doc["task_statuses"]

    def test_summary_event_component_type_is_mwaa(self) -> None:
        """The summary Log_Event must have component_type='mwaa'."""
        from blog.code.dag_callbacks import write_dag_summary

        response_mock = MagicMock()
        response_mock.raise_for_status.return_value = None
        shipper_mock = MagicMock()
        shipper_mock._endpoint = "https://search-test.us-east-1.es.amazonaws.com"
        shipper_mock._index_name = "etl-logs"
        shipper_mock._auth = MagicMock()

        context = _make_success_context()
        posted_bodies: list[dict] = []

        def capture_post(url, data, **kwargs):
            posted_bodies.append(json.loads(data))
            return response_mock

        with (
            patch("blog.code.dag_callbacks.Variable.get", side_effect=_variable_get_side_effect),
            patch("blog.code.dag_callbacks.boto3.client"),
            patch("blog.code.dag_callbacks._get_log_shipper", return_value=shipper_mock),
            patch("blog.code.dag_callbacks.requests.post", side_effect=capture_post),
        ):
            write_dag_summary(context)

        assert posted_bodies[0]["component_type"] == "mwaa"

    def test_log_shipper_failure_does_not_raise(self) -> None:
        """If the raw POST raises, write_dag_summary must swallow the error."""
        from blog.code.dag_callbacks import write_dag_summary

        shipper_mock = MagicMock()
        shipper_mock._endpoint = "https://search-test.us-east-1.es.amazonaws.com"
        shipper_mock._index_name = "etl-logs"
        shipper_mock._auth = MagicMock()

        context = _make_success_context()

        with (
            patch("blog.code.dag_callbacks.Variable.get", side_effect=_variable_get_side_effect),
            patch("blog.code.dag_callbacks.boto3.client"),
            patch("blog.code.dag_callbacks._get_log_shipper", return_value=shipper_mock),
            patch("blog.code.dag_callbacks.requests.post", side_effect=Exception("OpenSearch unreachable")),
        ):
            # Should not raise
            write_dag_summary(context)

    def test_get_log_shipper_failure_does_not_raise(self) -> None:
        """If _get_log_shipper raises, write_dag_summary must swallow the error."""
        from blog.code.dag_callbacks import write_dag_summary

        context = _make_success_context()

        with (
            patch("blog.code.dag_callbacks.Variable.get", side_effect=_variable_get_side_effect),
            patch("blog.code.dag_callbacks.boto3.client"),
            patch(
                "blog.code.dag_callbacks._get_log_shipper",
                side_effect=Exception("Secrets Manager unavailable"),
            ),
        ):
            # Should not raise
            write_dag_summary(context)
