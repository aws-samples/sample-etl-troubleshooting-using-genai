"""
AWS Lambda function handler for the ETL transform step.

Accepts an event dict containing ``run_id`` and ``input_path``, performs
placeholder processing, and ships a structured LogEvent to OpenSearch
before returning.

Log shipping failures are caught and swallowed so that a shipping error
never causes the Lambda invocation itself to fail.

Requirements: 3.5
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so we can import from plugins/
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _read_opensearch_config() -> tuple[str, str, str]:
    """
    Read OpenSearch connection parameters from environment variables.

    Returns:
        (endpoint, index_name, region)
    """
    endpoint = os.environ.get("OPENSEARCH_ENDPOINT", "https://localhost:9200")
    index_name = os.environ.get("OPENSEARCH_INDEX", "etl-logs")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    return endpoint, index_name, region


def handler(event: dict, context) -> dict:
    """
    Lambda function entry point.

    Args:
        event: Dict containing at minimum ``run_id`` and ``input_path``.
        context: Lambda context object (provides ``aws_request_id``).

    Returns:
        ``{"statusCode": 200, "body": "success"}``
    """
    start_time = datetime.utcnow()

    run_id: str = event.get("run_id", "unknown")
    input_path: str = event.get("input_path", "")

    # Lambda runtime sets this env var automatically.
    function_name: str = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "etl-transform")
    request_id: str = getattr(context, "aws_request_id", "unknown")

    log.info(
        "Lambda handler invoked: function=%s request_id=%s run_id=%s input_path=%s",
        function_name,
        request_id,
        run_id,
        input_path,
    )

    # ------------------------------------------------------------------
    # Placeholder processing
    # ------------------------------------------------------------------
    log.info("Processing input from %s", input_path)
    # Real implementation would perform data transformation here.

    end_time = datetime.utcnow()
    duration_ms = int((end_time - start_time).total_seconds() * 1000)
    message = (
        f"Lambda function {function_name!r} completed successfully. "
        f"run_id={run_id!r} duration_ms={duration_ms}"
    )
    log.info(message)

    # ------------------------------------------------------------------
    # Ship LogEvent to OpenSearch (non-fatal on failure)
    # ------------------------------------------------------------------
    try:
        from plugins.log_shipper.models import LogEvent
        from plugins.log_shipper.log_shipper import LogShipper

        endpoint, index_name, region = _read_opensearch_config()

        log_event = LogEvent(
            run_id=run_id,
            task_name="lambda_transform",
            component_type="lambda",
            log_level="INFO",
            message=message,
            timestamp=end_time.isoformat() + "Z",
            start_time=start_time.isoformat() + "Z",
            end_time=end_time.isoformat() + "Z",
            duration_ms=duration_ms,
            function_name=function_name,
            request_id=request_id,
            outcome="success",
        )

        shipper = LogShipper(
            opensearch_endpoint=endpoint,
            index_name=index_name,
            region=region,
        )
        shipper.ship(log_event)
        log.info("LogEvent shipped to OpenSearch successfully.")
    except Exception as exc:  # noqa: BLE001
        # Log shipping failure must never fail the Lambda invocation.
        log.error("Failed to ship LogEvent to OpenSearch: %s", exc)

    return {"statusCode": 200, "body": "success"}
