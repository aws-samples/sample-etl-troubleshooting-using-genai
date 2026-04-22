"""
lambda_handler_example.py — AWS Lambda handler showing LogShipper call-site usage.

This snippet demonstrates how to integrate the LogShipper into an AWS Lambda
function.  The LogShipper is instantiated once outside the handler (module
scope) so that the boto3 session and SigV4 credentials are reused across warm
invocations.  The OpenSearch endpoint is retrieved from Secrets Manager during
the cold-start initialisation.

Compatible with Python 3.9+.

Dependencies (add to a Lambda layer or include in the deployment package):
    boto3>=1.34.0
    requests>=2.31.0
    requests-aws4auth==1.3.1

Required environment variables:
    SECRET_NAME   — Secrets Manager secret name holding the OpenSearch endpoint
    INDEX_NAME    — OpenSearch index name, e.g. "etl-logs-2024-03"
    AWS_REGION    — Automatically set by the Lambda runtime
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

from blog.code.log_event import LogEvent
from blog.code.log_shipper import LogShipper, LogShipperError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Cold-start initialisation — runs once per Lambda execution environment
# ---------------------------------------------------------------------------

_SECRET_NAME = os.environ["SECRET_NAME"]
_INDEX_NAME = os.environ["INDEX_NAME"]
_REGION = os.environ.get("AWS_REGION", "us-east-1")

_opensearch_endpoint = LogShipper._get_endpoint_from_secrets_manager(_SECRET_NAME)

_shipper = LogShipper(
    opensearch_endpoint=_opensearch_endpoint,
    index_name=_INDEX_NAME,
    region=_REGION,
)

# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entry point.

    Parameters
    ----------
    event : dict
        The event payload passed by the invoker.  Expected keys:
            run_id    — MWAA DAG run ID (correlation key)
            task_name — Airflow task_id that triggered this invocation
    context : LambdaContext
        The Lambda runtime context object.

    Returns
    -------
    dict
        A response dict with ``statusCode`` and ``body``.
    """
    run_id: str = event.get("run_id", "unknown")
    task_name: str = event.get("task_name", "lambda_transform")

    start_time = datetime.now(timezone.utc)
    outcome = "success"
    result_body: Dict[str, Any] = {}

    try:
        # --- Your Lambda business logic goes here ---
        logger.info("Processing event for run_id=%s task_name=%s", run_id, task_name)

        # Example: lightweight data transformation
        records = event.get("records", [])
        processed = [{"id": r["id"], "value": r["value"] * 2} for r in records]
        result_body = {"processed_count": len(processed)}

        logger.info("Lambda completed successfully: %d records processed", len(processed))

    except Exception as exc:  # noqa: BLE001
        outcome = "error"
        logger.exception("Lambda function failed: %s", exc)
        # Re-raise after shipping the log event so the invoker sees the error
        raise

    finally:
        end_time = datetime.now(timezone.utc)
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        # -------------------------------------------------------------------
        # Ship the terminal log event to OpenSearch.
        # Wrap in try/except so a shipping failure does not crash the handler.
        # -------------------------------------------------------------------
        log_event = LogEvent(
            run_id=run_id,
            task_name=task_name,
            component_type="lambda",
            log_level="INFO" if outcome == "success" else "ERROR",
            message=(
                "Lambda function completed successfully"
                if outcome == "success"
                else "Lambda function failed"
            ),
            timestamp=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            # Lambda-specific fields
            function_name=context.function_name,
            request_id=context.aws_request_id,
            outcome=outcome,
            # Timing fields
            start_time=start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_time=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            duration_ms=duration_ms,
        )

        try:
            _shipper.ship(log_event)
        except LogShipperError as ship_err:
            # Log shipping failed — emit a warning but do NOT re-raise so the
            # handler outcome is determined solely by the business logic above.
            logger.warning("LogShipper: failed to ship log event: %s", ship_err)

    return {
        "statusCode": 200,
        "body": result_body,
    }
