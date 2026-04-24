"""
AWS Glue ETL job script — MWAA-compatible version.

Uses flat imports (no plugins.* package path) so it works when deployed
as a standalone script to Glue without a custom Python library path.

Compatible with Glue Python Shell (Python 3.9+).
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Glue-specific imports
# ---------------------------------------------------------------------------
try:
    from awsglue.utils import getResolvedOptions
    _IN_GLUE = True
except ImportError:
    _IN_GLUE = False


def _parse_args() -> dict:
    arg_names = ["JOB_NAME", "run_id", "source_path", "target_path", "partition_date"]
    if _IN_GLUE:
        return getResolvedOptions(sys.argv, arg_names)
    return {
        "JOB_NAME": os.environ.get("JOB_NAME", "etl-extraction-job"),
        "run_id": os.environ.get("run_id", "unknown"),
        "source_path": os.environ.get("source_path", "s3://etl-source/"),
        "target_path": os.environ.get("target_path", "s3://etl-target/"),
        "partition_date": os.environ.get("partition_date", ""),
    }


def main() -> None:
    args = _parse_args()
    job_name = args["JOB_NAME"]
    run_id = args["run_id"]
    source_path = args["source_path"]
    target_path = args["target_path"]
    partition_date = args["partition_date"]
    job_run_id = os.environ.get("JOB_RUN_ID", "unknown")

    start_time = datetime.now(timezone.utc)
    log.info("Glue job starting: job=%s run_id=%s", job_name, run_id)

    # Placeholder ETL logic
    log.info("Processing %s -> %s partition=%s", source_path, target_path, partition_date)

    end_time = datetime.now(timezone.utc)
    duration_ms = int((end_time - start_time).total_seconds() * 1000)
    message = f"Glue job {job_name!r} completed successfully. duration_ms={duration_ms}"
    log.info(message)

    # Ship LogEvent to OpenSearch
    try:
        import boto3
        import requests
        from requests_aws4auth import AWS4Auth

        # Read from Glue job args (--OPENSEARCH_ENDPOINT) or fall back to env var
        os_endpoint_key = "--OPENSEARCH_ENDPOINT"
        os_index_key = "--OPENSEARCH_INDEX"
        glue_args = {}
        if _IN_GLUE:
            try:
                glue_args = getResolvedOptions(sys.argv, ["OPENSEARCH_ENDPOINT", "OPENSEARCH_INDEX"])
            except Exception:
                pass

        endpoint = glue_args.get("OPENSEARCH_ENDPOINT") or os.environ.get(
            "OPENSEARCH_ENDPOINT",
            "https://search-etl-monitoring-mz7zpvh76up33iejfbl7ock3oq.us-east-1.es.amazonaws.com"
        )
        index_name = glue_args.get("OPENSEARCH_INDEX") or os.environ.get("OPENSEARCH_INDEX", "etl-logs-2026-04")
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

        session = boto3.session.Session()
        credentials = session.get_credentials().get_frozen_credentials()
        auth = AWS4Auth(credentials.access_key, credentials.secret_key, region, "es",
                        session_token=credentials.token)

        event = {
            "run_id": run_id,
            "task_name": "glue_extraction",
            "component_type": "glue",
            "log_level": "INFO",
            "message": message,
            "timestamp": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "duration_ms": duration_ms,
            "job_name": job_name,
            "job_run_id": job_run_id,
            "terminal_status": "SUCCEEDED",
        }

        url = f"{endpoint.rstrip('/')}/{index_name}/_doc"
        response = requests.post(url, data=json.dumps(event), auth=auth,
                                 headers={"Content-Type": "application/json"})
        response.raise_for_status()
        log.info("LogEvent shipped to OpenSearch successfully.")
    except Exception as exc:
        log.error("Failed to ship LogEvent to OpenSearch: %s", exc)


if __name__ == "__main__":
    main()
