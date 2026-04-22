"""
dag_callbacks.py — Airflow DAG callback functions for the MWAA + OpenSearch
Monitoring Blog.

This module contains the ``notify_failure`` and ``write_dag_summary`` callback
functions that are attached to the ``etl_workflow`` DAG.  Separating them from
the DAG definition file makes them independently testable without importing the
Airflow operator classes (which require a full MWAA environment).

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5

Compatible with Python 3.9+.

Dependencies
------------
- apache-airflow>=2.6.0
- boto3>=1.34.0
- requests>=2.31.0
- requests-aws4auth==1.3.1
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

import boto3
import requests
from airflow.models import Variable

from log_event import LogEvent
from log_shipper import LogShipper

# ---------------------------------------------------------------------------
# Module-level logger — used inside callbacks to record log-shipping errors
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: build a LogShipper from Airflow Variables / Secrets Manager
# ---------------------------------------------------------------------------


def _get_log_shipper() -> LogShipper:
    """Instantiate a ``LogShipper`` using the OpenSearch endpoint stored in
    AWS Secrets Manager.

    The secret name is read from the Airflow Variable ``opensearch_secret_name``
    (default: ``"etl/opensearch/endpoint"``).  The index name is read from the
    Airflow Variable ``opensearch_index_name`` (default: ``"etl-logs"``).
    The AWS region is read from the Airflow Variable ``aws_region``
    (default: ``"us-east-1"``).

    Secrets are never embedded in DAG code, environment variables, or source
    control — they are fetched at runtime via ``secretsmanager:GetSecretValue``.
    """
    secret_name = Variable.get("opensearch_secret_name", default_var="etl/opensearch/endpoint")
    index_name = Variable.get("opensearch_index_name", default_var="etl-logs")
    region = Variable.get("aws_region", default_var="us-east-1")

    endpoint = LogShipper._get_endpoint_from_secrets_manager(secret_name)
    return LogShipper(
        opensearch_endpoint=endpoint,
        index_name=index_name,
        region=region,
    )


# ---------------------------------------------------------------------------
# Callback: task-level on_failure_callback
# ---------------------------------------------------------------------------


def notify_failure(context: Dict[str, Any]) -> None:
    """Publish an SNS failure notification and write a ``Log_Event`` to OpenSearch.

    This function is attached to every task via ``default_args["on_failure_callback"]``.
    It fires whenever a task exhausts all retries and transitions to the ``failed`` state.

    The SNS message and the OpenSearch ``Log_Event`` both contain:
    - ``run_id``         — Airflow DAG run ID (correlation key)
    - ``task_name``      — Airflow task_id of the failed task
    - ``failure_reason`` — string representation of the exception
    - ``timestamp``      — ISO 8601 UTC string of the failure time

    Parameters
    ----------
    context : dict
        Airflow callback context dictionary.  Key fields used:
        - ``context["run_id"]``            — DAG run ID
        - ``context["task_instance"]``     — TaskInstance object
        - ``context["exception"]``         — Exception that caused the failure

    Notes
    -----
    All ``LogShipper`` and SNS calls are wrapped in ``try/except`` so that a
    log-shipping or notification failure is recorded in the Airflow task log
    rather than re-raising and masking the original pipeline failure.
    """
    run_id: str = context.get("run_id", "unknown")
    task_instance = context.get("task_instance")
    task_name: str = task_instance.task_id if task_instance else "unknown"
    exception = context.get("exception")
    failure_reason: str = str(exception) if exception else "unknown error"
    timestamp: str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ------------------------------------------------------------------
    # 1. Publish SNS failure notification
    # ------------------------------------------------------------------
    sns_topic_arn: str = Variable.get("sns_failure_topic_arn")
    sns_message = json.dumps(
        {
            "run_id": run_id,
            "task_name": task_name,
            "failure_reason": failure_reason,
            "timestamp": timestamp,
        }
    )
    try:
        sns_client = boto3.client("sns")
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=sns_message,
            Subject=f"ETL Pipeline Failure: {task_name}",
        )
        log.info("notify_failure: SNS notification published for task '%s'", task_name)
    except Exception as sns_exc:  # noqa: BLE001
        # Log the SNS error but do not re-raise — the original task failure
        # is the important signal; a notification failure must not mask it.
        log.error(
            "notify_failure: failed to publish SNS notification for task '%s': %s",
            task_name,
            sns_exc,
        )

    # ------------------------------------------------------------------
    # 2. Write a Log_Event to OpenSearch
    # ------------------------------------------------------------------
    try:
        shipper = _get_log_shipper()
        event = LogEvent(
            run_id=run_id,
            task_name=task_name,
            component_type="mwaa",
            log_level="ERROR",
            message=f"Task '{task_name}' failed: {failure_reason}",
            timestamp=timestamp,
        )
        shipper.ship(event)
        log.info(
            "notify_failure: Log_Event shipped to OpenSearch for task '%s'", task_name
        )
    except Exception as ship_exc:  # noqa: BLE001
        # Log the shipping error to the Airflow task log; do NOT re-raise.
        # A log-shipping failure must never mask the original pipeline failure.
        log.error(
            "notify_failure: failed to ship Log_Event to OpenSearch for task '%s': %s",
            task_name,
            ship_exc,
        )


# ---------------------------------------------------------------------------
# Callback: DAG-level on_success_callback
# ---------------------------------------------------------------------------


def write_dag_summary(context: Dict[str, Any]) -> None:
    """Write a summary ``Log_Event`` to OpenSearch when the DAG run completes.

    This function is attached at the DAG level via the ``on_success_callback``
    parameter.  It fires once the final task in the DAG succeeds.

    The summary ``Log_Event`` contains:
    - ``run_id``          — Airflow DAG run ID (correlation key)
    - ``overall_status``  — ``"success"``
    - ``total_duration``  — wall-clock duration of the DAG run in milliseconds
    - ``task_statuses``   — dict mapping each task_id to its terminal state string

    Parameters
    ----------
    context : dict
        Airflow callback context dictionary.  Key fields used:
        - ``context["run_id"]``       — DAG run ID
        - ``context["dag_run"]``      — DagRun object
        - ``context["dag"]``          — DAG object

    Notes
    -----
    All ``LogShipper`` calls are wrapped in ``try/except`` so that a
    log-shipping failure is recorded in the Airflow task log rather than
    re-raising and masking the original pipeline outcome.
    """
    run_id: str = context.get("run_id", "unknown")
    dag_run = context.get("dag_run")
    now_utc = datetime.now(tz=timezone.utc)

    # ------------------------------------------------------------------
    # Compute total_duration from dag_run.start_date → now
    # ------------------------------------------------------------------
    start_dt: datetime
    if dag_run and dag_run.start_date:
        raw = dag_run.start_date
        start_dt = (
            raw.replace(tzinfo=timezone.utc) if raw.tzinfo is None else raw
        )
    else:
        start_dt = now_utc

    total_duration_ms: int = int((now_utc - start_dt).total_seconds() * 1000)
    start_time_str: str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time_str: str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ------------------------------------------------------------------
    # Collect per-task terminal states
    # ------------------------------------------------------------------
    task_statuses: Dict[str, str] = {}
    if dag_run:
        for ti in dag_run.get_task_instances():
            task_statuses[ti.task_id] = ti.state or "none"

    # ------------------------------------------------------------------
    # Build and ship the summary Log_Event
    # ------------------------------------------------------------------
    try:
        shipper = _get_log_shipper()
        event = LogEvent(
            run_id=run_id,
            task_name="dag_summary",
            component_type="mwaa",
            log_level="INFO",
            message="DAG run completed successfully",
            timestamp=end_time_str,
            start_time=start_time_str,
            end_time=end_time_str,
            duration_ms=total_duration_ms,
        )
        # Augment the serialised document with the summary-specific fields
        # (overall_status, task_statuses) that are not part of the base
        # LogEvent schema but are required by Requirement 6.3.
        doc = event.to_dict()
        doc["overall_status"] = "success"
        doc["task_statuses"] = task_statuses

        url = f"{shipper._endpoint}/{shipper._index_name}/_doc"
        response = requests.post(
            url,
            data=json.dumps(doc),
            headers={"Content-Type": "application/json"},
            auth=shipper._auth,
            timeout=10,
        )
        response.raise_for_status()
        log.info(
            "write_dag_summary: summary Log_Event shipped to OpenSearch for run '%s'",
            run_id,
        )
    except Exception as ship_exc:  # noqa: BLE001
        # Log the shipping error to the Airflow task log; do NOT re-raise.
        log.error(
            "write_dag_summary: failed to ship summary Log_Event to OpenSearch "
            "for run '%s': %s",
            run_id,
            ship_exc,
        )
