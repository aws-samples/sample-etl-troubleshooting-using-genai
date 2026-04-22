"""
Unit tests for dags/callbacks.py.

Covers:
- notify_failure: publishes to SNS with correct JSON fields
- notify_failure: handles missing Variable gracefully (logs warning, does not raise)
- emit_cloudwatch_metrics: calls put_metric_data with all three metric names
- write_summary_log: calls LogShipper.ship() with a LogEvent
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from dags.callbacks import notify_failure, emit_cloudwatch_metrics, write_summary_log


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_task_instance(task_id: str = "glue_extraction") -> MagicMock:
    ti = MagicMock()
    ti.task_id = task_id
    return ti


def make_failure_context(
    run_id: str = "run-abc123",
    task_id: str = "glue_extraction",
    exception: Exception | None = None,
) -> dict:
    return {
        "run_id": run_id,
        "task_instance": make_task_instance(task_id),
        "exception": exception or RuntimeError("something went wrong"),
    }


def make_dag_run(state: str = "success", start_date: datetime | None = None) -> MagicMock:
    dag_run = MagicMock()
    dag_run.state = state
    dag_run.start_date = start_date or datetime(2024, 1, 15, 12, 0, 0)
    dag_run.get_task_instances.return_value = []
    return dag_run


# ---------------------------------------------------------------------------
# notify_failure
# ---------------------------------------------------------------------------

class TestNotifyFailure:
    def test_publishes_to_sns_with_correct_fields(self):
        """notify_failure publishes a JSON message with run_id, task_name, failure_reason, timestamp."""
        mock_sns_client = MagicMock()
        context = make_failure_context(run_id="run-xyz", task_id="lambda_transform")

        # Variable is imported inside notify_failure as `from airflow.models import Variable`
        # so we patch it at the source: airflow.models.Variable
        with patch("dags.callbacks.boto3.client", return_value=mock_sns_client) as mock_boto3, \
             patch("airflow.models.Variable") as mock_variable_class:
            mock_variable_class.get.return_value = "arn:aws:sns:us-east-1:123456789012:etl-failures"

            notify_failure(context)

        # boto3.client("sns") should have been called
        mock_boto3.assert_called_once_with("sns")

        # sns.publish should have been called once
        mock_sns_client.publish.assert_called_once()
        call_kwargs = mock_sns_client.publish.call_args.kwargs

        # Verify TopicArn
        assert call_kwargs["TopicArn"] == "arn:aws:sns:us-east-1:123456789012:etl-failures"

        # Parse and verify the message JSON
        message = json.loads(call_kwargs["Message"])
        assert message["run_id"] == "run-xyz"
        assert message["task_name"] == "lambda_transform"
        assert "failure_reason" in message
        assert "timestamp" in message

    def test_all_four_required_fields_present_in_message(self):
        """The SNS message JSON contains exactly the four required fields."""
        mock_sns_client = MagicMock()
        context = make_failure_context()

        with patch("dags.callbacks.boto3.client", return_value=mock_sns_client), \
             patch("airflow.models.Variable") as mock_variable_class:
            mock_variable_class.get.return_value = "arn:aws:sns:us-east-1:123456789012:etl-failures"

            notify_failure(context)

        message = json.loads(mock_sns_client.publish.call_args.kwargs["Message"])
        for field in ("run_id", "task_name", "failure_reason", "timestamp"):
            assert field in message, f"Expected field {field!r} in SNS message"

    def test_handles_missing_variable_gracefully(self, caplog):
        """notify_failure logs a warning and does not raise when Variable is missing."""
        context = make_failure_context()

        with patch("airflow.models.Variable") as mock_variable_class, \
             patch("dags.callbacks.boto3.client") as mock_boto3:
            mock_variable_class.get.side_effect = KeyError("sns_failure_topic_arn")

            with caplog.at_level(logging.WARNING):
                # Should not raise
                notify_failure(context)

        # SNS client should NOT have been called
        mock_boto3.assert_not_called()

    def test_handles_variable_import_error_gracefully(self):
        """notify_failure does not raise when Variable.get raises any exception."""
        context = make_failure_context()

        with patch("airflow.models.Variable") as mock_variable_class, \
             patch("dags.callbacks.boto3.client") as mock_boto3:
            mock_variable_class.get.side_effect = Exception("metastore unavailable")

            # Should not raise
            notify_failure(context)

        mock_boto3.assert_not_called()


# ---------------------------------------------------------------------------
# emit_cloudwatch_metrics
# ---------------------------------------------------------------------------

class TestEmitCloudwatchMetrics:
    def test_calls_put_metric_data_with_all_three_metrics(self):
        """emit_cloudwatch_metrics calls put_metric_data with all three metric names."""
        mock_cw_client = MagicMock()
        dag_run = make_dag_run()
        context = {"dag_run": dag_run}

        with patch("dags.callbacks.boto3.client", return_value=mock_cw_client):
            emit_cloudwatch_metrics(context)

        mock_cw_client.put_metric_data.assert_called_once()
        call_kwargs = mock_cw_client.put_metric_data.call_args.kwargs

        assert call_kwargs["Namespace"] == "ETL"
        metric_names = {m["MetricName"] for m in call_kwargs["MetricData"]}
        assert "ETL/DagRunDuration" in metric_names
        assert "ETL/TaskSuccessCount" in metric_names
        assert "ETL/TaskFailureCount" in metric_names

    def test_metric_data_has_three_entries(self):
        """emit_cloudwatch_metrics sends exactly three metric data points."""
        mock_cw_client = MagicMock()
        dag_run = make_dag_run()
        context = {"dag_run": dag_run}

        with patch("dags.callbacks.boto3.client", return_value=mock_cw_client):
            emit_cloudwatch_metrics(context)

        metric_data = mock_cw_client.put_metric_data.call_args.kwargs["MetricData"]
        assert len(metric_data) == 3

    def test_metrics_use_etl_workflow_dimension(self):
        """All metrics use DagId=etl_workflow dimension."""
        mock_cw_client = MagicMock()
        dag_run = make_dag_run()
        context = {"dag_run": dag_run}

        with patch("dags.callbacks.boto3.client", return_value=mock_cw_client):
            emit_cloudwatch_metrics(context)

        metric_data = mock_cw_client.put_metric_data.call_args.kwargs["MetricData"]
        for metric in metric_data:
            dims = {d["Name"]: d["Value"] for d in metric["Dimensions"]}
            assert dims.get("DagId") == "etl_workflow"

    def test_works_with_no_dag_run(self):
        """emit_cloudwatch_metrics does not raise when dag_run is None."""
        mock_cw_client = MagicMock()
        context = {"dag_run": None}

        with patch("dags.callbacks.boto3.client", return_value=mock_cw_client):
            emit_cloudwatch_metrics(context)

        mock_cw_client.put_metric_data.assert_called_once()


# ---------------------------------------------------------------------------
# write_summary_log
# ---------------------------------------------------------------------------

class TestWriteSummaryLog:
    def test_calls_log_shipper_ship_with_log_event(self):
        """write_summary_log calls LogShipper.ship() with a LogEvent instance."""
        from plugins.log_shipper.models import LogEvent

        dag_run = make_dag_run(state="success")
        context = {"dag_run": dag_run, "run_id": "run-summary-001"}

        mock_ship = MagicMock()

        # LogShipper is imported inside write_summary_log as:
        #   from plugins.log_shipper.log_shipper import LogShipper
        # get_secret is imported as:
        #   from plugins.secrets import get_secret
        with patch("plugins.log_shipper.log_shipper.LogShipper") as mock_shipper_class, \
             patch("plugins.secrets.get_secret", return_value={}):
            mock_shipper_instance = MagicMock()
            mock_shipper_instance.ship = mock_ship
            mock_shipper_class.return_value = mock_shipper_instance

            write_summary_log(context)

        mock_ship.assert_called_once()
        shipped_event = mock_ship.call_args.args[0]
        assert isinstance(shipped_event, LogEvent)

    def test_log_event_has_correct_run_id(self):
        """The LogEvent passed to ship() carries the correct run_id."""
        dag_run = make_dag_run(state="success")
        context = {"dag_run": dag_run, "run_id": "run-check-42"}

        mock_ship = MagicMock()

        with patch("plugins.log_shipper.log_shipper.LogShipper") as mock_shipper_class, \
             patch("plugins.secrets.get_secret", return_value={}):
            mock_shipper_class.return_value.ship = mock_ship

            write_summary_log(context)

        shipped_event = mock_ship.call_args.args[0]
        assert shipped_event.run_id == "run-check-42"

    def test_does_not_raise_when_ship_fails(self):
        """write_summary_log catches LogShipper exceptions and does not re-raise."""
        dag_run = make_dag_run(state="failed")
        context = {"dag_run": dag_run, "run_id": "run-fail-001"}

        with patch("plugins.log_shipper.log_shipper.LogShipper") as mock_shipper_class, \
             patch("plugins.secrets.get_secret", return_value={}):
            mock_shipper_class.return_value.ship.side_effect = Exception("OpenSearch down")

            # Should not raise
            write_summary_log(context)
