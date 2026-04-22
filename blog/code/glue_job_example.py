"""
glue_job_example.py — AWS Glue job script showing LogShipper call-site usage.

This snippet demonstrates how to integrate the LogShipper into an AWS Glue
PySpark job.  The OpenSearch endpoint is retrieved from Secrets Manager at
runtime so that no credentials or endpoints are hardcoded in the script.

Compatible with Python 3.9+.

Dependencies (add to the Glue job's Python library path or --additional-python-modules):
    boto3>=1.34.0
    requests>=2.31.0
    requests-aws4auth==1.3.1
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from blog.code.log_event import LogEvent
from blog.code.log_shipper import LogShipper, LogShipperError

# ---------------------------------------------------------------------------
# Glue job bootstrap
# ---------------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "run_id",          # passed from the MWAA DAG via --run_id
        "task_name",       # e.g. "glue_extraction"
        "secret_name",     # Secrets Manager secret that holds the OS endpoint
        "index_name",      # e.g. "etl-logs-2024-03"
        "region",          # e.g. "us-east-1"
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# Retrieve the OpenSearch endpoint from Secrets Manager
# ---------------------------------------------------------------------------

opensearch_endpoint = LogShipper._get_endpoint_from_secrets_manager(
    args["secret_name"]
)

# ---------------------------------------------------------------------------
# Instantiate LogShipper
# ---------------------------------------------------------------------------

shipper = LogShipper(
    opensearch_endpoint=opensearch_endpoint,
    index_name=args["index_name"],
    region=args["region"],
)

# ---------------------------------------------------------------------------
# Main job logic
# ---------------------------------------------------------------------------

start_time = datetime.now(timezone.utc)

# Retrieve the Glue job run ID from the Glue context
glue_job_run_id = args.get("JOB_RUN_ID", "unknown")

try:
    # --- Your ETL logic goes here ---
    # Example: read from S3, transform, write to another S3 location
    datasource = glue_context.create_dynamic_frame.from_catalog(
        database="my_database",
        table_name="raw_events",
    )
    # ... transformation steps ...
    # glue_context.write_dynamic_frame.from_options(...)

    end_time = datetime.now(timezone.utc)
    duration_ms = int((end_time - start_time).total_seconds() * 1000)
    terminal_status = "SUCCEEDED"
    log_level = "INFO"
    message = "Glue job completed successfully"

except Exception as exc:  # noqa: BLE001
    end_time = datetime.now(timezone.utc)
    duration_ms = int((end_time - start_time).total_seconds() * 1000)
    terminal_status = "FAILED"
    log_level = "ERROR"
    message = f"Glue job failed: {exc}"
    # Re-raise after shipping the log event so Glue marks the run as FAILED
    raise

finally:
    # -----------------------------------------------------------------------
    # Ship the terminal log event to OpenSearch.
    # Wrap in try/except so a shipping failure does not crash the Glue job.
    # -----------------------------------------------------------------------
    event = LogEvent(
        run_id=args["run_id"],
        task_name=args["task_name"],
        component_type="glue",
        log_level=log_level,
        message=message,
        timestamp=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        # Glue-specific fields
        job_name=args["JOB_NAME"],
        job_run_id=glue_job_run_id,
        terminal_status=terminal_status,
        # Timing fields
        start_time=start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_time=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        duration_ms=duration_ms,
    )

    try:
        shipper.ship(event)
    except LogShipperError as ship_err:
        # Log shipping failed — print to stderr so the error appears in
        # CloudWatch Logs, but do NOT re-raise so the job outcome is
        # determined solely by the ETL logic above.
        print(f"[LogShipper] WARNING: failed to ship log event: {ship_err}", file=sys.stderr)

# ---------------------------------------------------------------------------
# Commit the Glue job bookmark
# ---------------------------------------------------------------------------

job.commit()
