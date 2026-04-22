"""
DAG callbacks for the ETL workflow.

Callbacks:
  - notify_failure          → on_failure_callback for each task (task 5.2)
  - emit_cloudwatch_metrics → DAG-level on_success_callback / on_failure_callback (task 5.4)
  - write_summary_log       → DAG-level on_success_callback / on_failure_callback (task 5.6)
  - log_task_success        → on_success_callback per task (task 10.2)
  - log_task_retry          → on_retry_callback per task (task 10.2)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

import boto3

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task 5.2 — SNS failure notification
# ---------------------------------------------------------------------------

def notify_failure(context: dict) -> None:
    """
    Publish an SNS failure notification when a task fails after exhausting retries.

    Reads the SNS topic ARN from the Airflow Variable ``sns_failure_topic_arn``.
    If the Variable is missing the function logs a warning and returns without
    raising so that a missing Variable never prevents the DAG from completing
    its own failure-handling path.

    The SNS message is a JSON string with exactly these fields:
        run_id, task_name, failure_reason, timestamp (ISO 8601 UTC)

    Requirements: 6.1
    """
    # Import inside the function so the module is importable without a live
    # Airflow metastore (the Variable.get call is what requires the DB).
    try:
        from airflow.models import Variable
        topic_arn = Variable.get("sns_failure_topic_arn")
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "notify_failure: could not retrieve sns_failure_topic_arn "
            "(Variable missing or metastore unavailable): %s",
            exc,
        )
        return

    run_id = context.get("run_id", "unknown")
    task_name = context["task_instance"].task_id
    failure_reason = str(context.get("exception", "unknown"))
    timestamp = datetime.utcnow().isoformat() + "Z"

    message = json.dumps(
        {
            "run_id": run_id,
            "task_name": task_name,
            "failure_reason": failure_reason,
            "timestamp": timestamp,
        }
    )

    sns = boto3.client("sns")
    sns.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject=f"ETL Pipeline Failure: {task_name}",
    )


# ---------------------------------------------------------------------------
# Task 5.4 — CloudWatch metrics
# ---------------------------------------------------------------------------

def emit_cloudwatch_metrics(context: dict) -> None:
    """
    Emit ETL/DagRunDuration, ETL/TaskSuccessCount, and ETL/TaskFailureCount
    CloudWatch metrics for the completed DAG run.

    Metrics are published under the ``ETL`` namespace with dimension
    ``DagId=etl_workflow``.

    Requirements: 8.1
    """
    dag_run = context.get("dag_run")
    now = datetime.utcnow()

    # Compute wall-clock duration in seconds.
    if dag_run and dag_run.start_date:
        start = dag_run.start_date
        # start_date may be timezone-aware; normalise to naive UTC for arithmetic.
        if hasattr(start, "tzinfo") and start.tzinfo is not None:
            from datetime import timezone
            start = start.astimezone(timezone.utc).replace(tzinfo=None)
        duration_seconds = (now - start).total_seconds()
    else:
        duration_seconds = 0.0

    # Count task instance states for this DAG run.
    success_count = 0
    failure_count = 0
    if dag_run:
        try:
            task_instances = dag_run.get_task_instances()
            for ti in task_instances:
                state = getattr(ti, "state", None)
                if state == "success":
                    success_count += 1
                elif state == "failed":
                    failure_count += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("emit_cloudwatch_metrics: could not retrieve task instances: %s", exc)

    dimensions = [{"Name": "DagId", "Value": "etl_workflow"}]

    metric_data = [
        {
            "MetricName": "ETL/DagRunDuration",
            "Dimensions": dimensions,
            "Value": duration_seconds,
            "Unit": "Seconds",
        },
        {
            "MetricName": "ETL/TaskSuccessCount",
            "Dimensions": dimensions,
            "Value": float(success_count),
            "Unit": "Count",
        },
        {
            "MetricName": "ETL/TaskFailureCount",
            "Dimensions": dimensions,
            "Value": float(failure_count),
            "Unit": "Count",
        },
    ]

    cw = boto3.client("cloudwatch")
    cw.put_metric_data(Namespace="ETL", MetricData=metric_data)


# ---------------------------------------------------------------------------
# Task 5.6 — Summary log to OpenSearch
# ---------------------------------------------------------------------------

def write_summary_log(context: dict) -> None:
    """
    Write a summary LogEvent to OpenSearch on DAG completion (success or failure).

    Reads OpenSearch configuration from Secrets Manager via ``get_secret``.
    If log shipping fails the exception is caught and logged so that a
    shipping failure never crashes the DAG.

    Requirements: 6.3
    """
    from plugins.secrets import get_secret
    from plugins.log_shipper.models import LogEvent
    from plugins.log_shipper.log_shipper import LogShipper

    dag_run = context.get("dag_run")
    now = datetime.utcnow()
    now_iso = now.isoformat() + "Z"

    run_id = context.get("run_id", "unknown")

    # Determine overall DAG outcome.
    dag_state = getattr(dag_run, "state", None) if dag_run else None
    # Airflow uses "success" for a successful DAG run.
    dag_succeeded = dag_state == "success"
    log_level = "INFO" if dag_succeeded else "ERROR"

    # Compute timing.
    start_time_iso: str | None = None
    duration_ms = 0
    if dag_run and dag_run.start_date:
        start = dag_run.start_date
        if hasattr(start, "tzinfo") and start.tzinfo is not None:
            from datetime import timezone
            start = start.astimezone(timezone.utc).replace(tzinfo=None)
        start_time_iso = start.isoformat() + "Z"
        duration_ms = int((now - start).total_seconds() * 1000)

    # Build human-readable summary message.
    status_word = "succeeded" if dag_succeeded else "failed"
    message = (
        f"DAG run {run_id} {status_word}. "
        f"Duration: {duration_ms} ms."
    )

    event = LogEvent(
        run_id=run_id,
        task_name="dag_summary",
        component_type="mwaa",
        log_level=log_level,
        message=message,
        timestamp=now_iso,
        start_time=start_time_iso,
        end_time=now_iso,
        duration_ms=duration_ms,
    )

    # Retrieve OpenSearch config from Secrets Manager (stub returns {} until task 11.1).
    try:
        opensearch_config = get_secret("etl/opensearch/endpoint")
        opensearch_endpoint = opensearch_config.get("endpoint", "https://localhost:9200")
        index_config = get_secret("etl/opensearch/index")
        index_name = index_config.get("index", "etl-logs")
        region = opensearch_config.get("region", "us-east-1")
    except Exception as exc:  # noqa: BLE001
        log.warning("write_summary_log: could not retrieve OpenSearch config: %s", exc)
        opensearch_endpoint = "https://localhost:9200"
        index_name = "etl-logs"
        region = "us-east-1"

    try:
        shipper = LogShipper(
            opensearch_endpoint=opensearch_endpoint,
            index_name=index_name,
            region=region,
        )
        shipper.ship(event)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "write_summary_log: failed to ship summary LogEvent (run_id=%r): %s",
            run_id,
            exc,
        )


# ---------------------------------------------------------------------------
# Task 10.2 — MWAA task lifecycle log shipping
# ---------------------------------------------------------------------------

def log_task_success(context: dict) -> None:
    """
    Ship a task-success LogEvent to OpenSearch when a task completes successfully.

    Uses component_type="mwaa" and log_level="INFO".  Failures in log shipping
    are caught and logged so they never crash the DAG.

    Requirements: 5.3
    """
    from plugins.log_shipper.models import LogEvent
    from plugins.log_shipper.log_shipper import LogShipper
    from plugins.secrets import get_secret

    task_name = context["task_instance"].task_id
    run_id = context.get("run_id", "unknown")
    now_iso = datetime.utcnow().isoformat() + "Z"

    event = LogEvent(
        run_id=run_id,
        task_name=task_name,
        component_type="mwaa",
        log_level="INFO",
        message=f"Task {task_name} succeeded",
        timestamp=now_iso,
    )

    try:
        opensearch_config = get_secret("etl/opensearch/endpoint")
        opensearch_endpoint = opensearch_config.get("endpoint", "https://localhost:9200")
        index_config = get_secret("etl/opensearch/index")
        index_name = index_config.get("index", "etl-logs")
        region = opensearch_config.get("region", "us-east-1")
    except Exception as exc:  # noqa: BLE001
        log.warning("log_task_success: could not retrieve OpenSearch config: %s", exc)
        opensearch_endpoint = "https://localhost:9200"
        index_name = "etl-logs"
        region = "us-east-1"

    try:
        shipper = LogShipper(
            opensearch_endpoint=opensearch_endpoint,
            index_name=index_name,
            region=region,
        )
        shipper.ship(event)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "log_task_success: failed to ship LogEvent (run_id=%r, task=%r): %s",
            run_id,
            task_name,
            exc,
        )


def log_task_retry(context: dict) -> None:
    """
    Ship a task-retry LogEvent to OpenSearch when a task is being retried.

    Uses component_type="mwaa" and log_level="WARN".  Failures in log shipping
    are caught and logged so they never crash the DAG.

    Requirements: 5.3
    """
    from plugins.log_shipper.models import LogEvent
    from plugins.log_shipper.log_shipper import LogShipper
    from plugins.secrets import get_secret

    task_name = context["task_instance"].task_id
    run_id = context.get("run_id", "unknown")
    now_iso = datetime.utcnow().isoformat() + "Z"

    event = LogEvent(
        run_id=run_id,
        task_name=task_name,
        component_type="mwaa",
        log_level="WARN",
        message=f"Task {task_name} is being retried",
        timestamp=now_iso,
    )

    try:
        opensearch_config = get_secret("etl/opensearch/endpoint")
        opensearch_endpoint = opensearch_config.get("endpoint", "https://localhost:9200")
        index_config = get_secret("etl/opensearch/index")
        index_name = index_config.get("index", "etl-logs")
        region = opensearch_config.get("region", "us-east-1")
    except Exception as exc:  # noqa: BLE001
        log.warning("log_task_retry: could not retrieve OpenSearch config: %s", exc)
        opensearch_endpoint = "https://localhost:9200"
        index_name = "etl-logs"
        region = "us-east-1"

    try:
        shipper = LogShipper(
            opensearch_endpoint=opensearch_endpoint,
            index_name=index_name,
            region=region,
        )
        shipper.ship(event)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "log_task_retry: failed to ship LogEvent (run_id=%r, task=%r): %s",
            run_id,
            task_name,
            exc,
        )
