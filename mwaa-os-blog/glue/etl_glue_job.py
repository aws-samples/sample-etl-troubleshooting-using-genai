"""
AWS Glue ETL job script.

This script is submitted to AWS Glue as a Python shell or Spark job.
It accepts standard Glue job arguments, performs placeholder ETL processing,
and ships a structured LogEvent to OpenSearch on completion.

Log shipping failures are caught and swallowed so that a shipping error
never causes the Glue job itself to fail.

Requirements: 2.6
"""

from __future__ import annotations

import os
import sys
import logging
from datetime import datetime

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so we can import from plugins/
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Glue-specific imports (available in the Glue runtime environment)
# ---------------------------------------------------------------------------
try:
    from awsglue.utils import getResolvedOptions
    import awsglue  # noqa: F401 — confirms we are in a real Glue environment
    _IN_GLUE = True
except ImportError:
    # Allow the script to be imported/tested outside a Glue environment.
    _IN_GLUE = False

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _parse_args() -> dict:
    """
    Parse Glue job arguments.

    In a real Glue environment, ``getResolvedOptions`` reads from
    ``sys.argv``.  Outside Glue (e.g. unit tests), we fall back to
    ``os.environ`` so the script remains testable.
    """
    arg_names = ["JOB_NAME", "run_id", "source_path", "target_path", "partition_date"]

    if _IN_GLUE:
        return getResolvedOptions(sys.argv, arg_names)

    # Fallback for non-Glue environments (tests / local runs).
    return {
        "JOB_NAME": os.environ.get("JOB_NAME", "etl-extraction-job"),
        "run_id": os.environ.get("run_id", "unknown"),
        "source_path": os.environ.get("source_path", "s3://etl-source/"),
        "target_path": os.environ.get("target_path", "s3://etl-target/"),
        "partition_date": os.environ.get("partition_date", ""),
    }


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


def main() -> None:
    """
    Main entry point for the Glue job.

    1. Parse job arguments.
    2. Record start time.
    3. Perform placeholder ETL processing.
    4. Record end time.
    5. Ship a LogEvent to OpenSearch (failure is non-fatal).
    """
    args = _parse_args()

    job_name: str = args["JOB_NAME"]
    run_id: str = args["run_id"]
    source_path: str = args["source_path"]
    target_path: str = args["target_path"]
    partition_date: str = args["partition_date"]

    # Glue sets JOB_RUN_ID as an environment variable during execution.
    job_run_id: str = os.environ.get("JOB_RUN_ID", "unknown")

    start_time = datetime.utcnow()
    log.info(
        "Glue job starting: job_name=%s run_id=%s source=%s target=%s partition=%s",
        job_name,
        run_id,
        source_path,
        target_path,
        partition_date,
    )

    # ------------------------------------------------------------------
    # Placeholder ETL processing
    # ------------------------------------------------------------------
    log.info("Processing data from %s to %s for partition %s", source_path, target_path, partition_date)
    # Real implementation would use GlueContext / DynamicFrame / Spark here.

    end_time = datetime.utcnow()
    duration_ms = int((end_time - start_time).total_seconds() * 1000)
    message = (
        f"Glue job {job_name!r} completed successfully. "
        f"run_id={run_id!r} partition_date={partition_date!r} "
        f"duration_ms={duration_ms}"
    )
    log.info(message)

    # ------------------------------------------------------------------
    # Ship LogEvent to OpenSearch (non-fatal on failure)
    # ------------------------------------------------------------------
    try:
        from plugins.log_shipper.models import LogEvent
        from plugins.log_shipper.log_shipper import LogShipper

        endpoint, index_name, region = _read_opensearch_config()

        event = LogEvent(
            run_id=run_id,
            task_name="glue_extraction",
            component_type="glue",
            log_level="INFO",
            message=message,
            timestamp=end_time.isoformat() + "Z",
            start_time=start_time.isoformat() + "Z",
            end_time=end_time.isoformat() + "Z",
            duration_ms=duration_ms,
            job_name=job_name,
            job_run_id=job_run_id,
            terminal_status="SUCCEEDED",
        )

        shipper = LogShipper(
            opensearch_endpoint=endpoint,
            index_name=index_name,
            region=region,
        )
        shipper.ship(event)
        log.info("LogEvent shipped to OpenSearch successfully.")
    except Exception as exc:  # noqa: BLE001
        # Log shipping failure must never fail the Glue job.
        log.error("Failed to ship LogEvent to OpenSearch: %s", exc)


if __name__ == "__main__":
    main()
