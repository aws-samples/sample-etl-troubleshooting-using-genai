"""
ec2_script_example.py — EC2 Python script showing LogShipper call-site usage.

This snippet demonstrates how to integrate the LogShipper into a Python script
that runs on an Amazon EC2 instance (invoked via AWS Systems Manager Run
Command or directly via SSH).  The OpenSearch endpoint is retrieved from
Secrets Manager at startup so that no credentials or endpoints are hardcoded
in the script or in SSM parameter values.

Compatible with Python 3.9+.

Dependencies (install on the EC2 instance or in a virtualenv):
    boto3>=1.34.0
    requests>=2.31.0
    requests-aws4auth==1.3.1

Usage (invoked by MWAA via SSM Run Command):
    python ec2_script_example.py \
        --run-id "scheduled__2024-03-15T10:00:00+00:00" \
        --task-name "ec2_custom_script" \
        --secret-name "etl/opensearch-config" \
        --index-name "etl-logs-2024-03" \
        --region "us-east-1"
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

import boto3

from blog.code.log_event import LogEvent
from blog.code.log_shipper import LogShipper, LogShipperError

# ---------------------------------------------------------------------------
# Logging setup — INFO to stdout, ERROR to stderr
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="EC2 custom processing script with OpenSearch log shipping"
    )
    parser.add_argument("--run-id", required=True, help="MWAA DAG run ID (correlation key)")
    parser.add_argument("--task-name", required=True, help="Airflow task_id for this script")
    parser.add_argument("--secret-name", required=True, help="Secrets Manager secret name")
    parser.add_argument("--index-name", required=True, help="OpenSearch index name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Retrieve the EC2 instance ID from the instance metadata service (IMDSv2)
# ---------------------------------------------------------------------------


def _get_instance_id() -> str:
    """Return the EC2 instance ID via IMDSv2, or 'unknown' if unavailable."""
    try:
        import urllib.request

        # Step 1: obtain a session token (IMDSv2 requirement)
        token_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            method="PUT",
        )
        with urllib.request.urlopen(token_req, timeout=2) as resp:
            token = resp.read().decode()

        # Step 2: use the token to fetch the instance ID
        id_req = urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(id_req, timeout=2) as resp:
            return resp.read().decode()
    except Exception:  # noqa: BLE001
        return "unknown"


# ---------------------------------------------------------------------------
# Main processing logic
# ---------------------------------------------------------------------------


def _run_etl_processing() -> None:
    """Placeholder for the actual EC2 ETL processing logic.

    Replace this function body with your real processing steps.
    Raise an exception to signal failure.
    """
    logger.info("Starting EC2 custom processing step")
    # Example: read from S3, apply custom transformations, write results
    # s3 = boto3.client("s3")
    # obj = s3.get_object(Bucket="my-bucket", Key="input/data.json")
    # ... process data ...
    # s3.put_object(Bucket="my-bucket", Key="output/result.json", Body=...)
    logger.info("EC2 custom processing step completed")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    args = _parse_args()

    # Retrieve the OpenSearch endpoint from Secrets Manager
    opensearch_endpoint = LogShipper._get_endpoint_from_secrets_manager(
        args.secret_name
    )

    # Instantiate LogShipper
    shipper = LogShipper(
        opensearch_endpoint=opensearch_endpoint,
        index_name=args.index_name,
        region=args.region,
    )

    # Resolve the EC2 instance ID for the log event
    instance_id = _get_instance_id()
    script_name = __file__.split("/")[-1]  # basename of this script file

    start_time = datetime.now(timezone.utc)
    exit_code = 0
    log_level = "INFO"
    message = "EC2 script completed successfully"

    try:
        _run_etl_processing()

    except Exception as exc:  # noqa: BLE001
        exit_code = 1
        log_level = "ERROR"
        message = f"EC2 script failed: {exc}"
        logger.exception("EC2 script encountered an error: %s", exc)

    finally:
        end_time = datetime.now(timezone.utc)
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        # -------------------------------------------------------------------
        # Ship the terminal log event to OpenSearch.
        # Wrap in try/except so a shipping failure does not crash the script.
        # -------------------------------------------------------------------
        event = LogEvent(
            run_id=args.run_id,
            task_name=args.task_name,
            component_type="ec2",
            log_level=log_level,
            message=message,
            timestamp=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            # EC2-specific fields
            instance_id=instance_id,
            script_name=script_name,
            exit_code=exit_code,
            # Timing fields
            start_time=start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_time=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            duration_ms=duration_ms,
        )

        try:
            shipper.ship(event)
        except LogShipperError as ship_err:
            # Log shipping failed — print to stderr but do NOT change the
            # exit code so the script outcome reflects the ETL logic only.
            print(
                f"[LogShipper] WARNING: failed to ship log event: {ship_err}",
                file=sys.stderr,
            )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
