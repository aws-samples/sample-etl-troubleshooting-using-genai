"""
EC2 custom ETL script.

Reads runtime parameters from environment variables, performs placeholder
processing, and ships a structured LogEvent to OpenSearch on completion.

Log shipping failures are caught and written to /var/log/etl-log-shipper.log
so that a shipping error never causes the script to exit with a non-zero code.

Requirements: 4.6
"""

from __future__ import annotations

import logging
import os
import subprocess
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

_LOG_SHIPPER_FALLBACK_PATH = "/var/log/etl-log-shipper.log"


def _get_instance_id() -> str:
    """
    Retrieve the EC2 instance ID from the instance metadata service.

    Falls back to the ``INSTANCE_ID`` environment variable, and then to
    ``"unknown"`` if neither is available (e.g. in test environments).
    """
    # Prefer the env var (set by SSM command or test harness).
    instance_id = os.environ.get("INSTANCE_ID", "")
    if instance_id:
        return instance_id

    # Try the IMDSv1 endpoint (no token required for simple reads).
    try:
        result = subprocess.run(
            ["curl", "-s", "--max-time", "2",
             "http://169.254.169.254/latest/meta-data/instance-id"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:  # noqa: BLE001
        pass

    return "unknown"


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


def _write_fallback_log(message: str) -> None:
    """Write a message to the fallback log file, ignoring any I/O errors."""
    try:
        with open(_LOG_SHIPPER_FALLBACK_PATH, "a") as fh:
            fh.write(message + "\n")
    except Exception:  # noqa: BLE001
        pass


def main() -> int:
    """
    Main entry point for the EC2 custom script.

    Returns:
        Exit code: 0 on success, non-zero on error.
    """
    start_time = datetime.utcnow()

    # ------------------------------------------------------------------
    # Read runtime parameters from environment variables
    # ------------------------------------------------------------------
    run_id: str = os.environ.get("RUN_ID", "unknown")
    input_path: str = os.environ.get("INPUT_PATH", "")
    output_path: str = os.environ.get("OUTPUT_PATH", "")
    instance_id: str = _get_instance_id()

    log.info(
        "EC2 custom script starting: instance_id=%s run_id=%s input=%s output=%s",
        instance_id,
        run_id,
        input_path,
        output_path,
    )

    # ------------------------------------------------------------------
    # Placeholder processing
    # ------------------------------------------------------------------
    print(f"Processing input from: {input_path}")
    print(f"Writing output to:     {output_path}")
    # Real implementation would perform custom ETL logic here.

    end_time = datetime.utcnow()
    duration_ms = int((end_time - start_time).total_seconds() * 1000)
    message = (
        f"EC2 custom script completed successfully. "
        f"run_id={run_id!r} instance_id={instance_id!r} duration_ms={duration_ms}"
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
            task_name="ec2_custom_script",
            component_type="ec2",
            log_level="INFO",
            message=message,
            timestamp=end_time.isoformat() + "Z",
            start_time=start_time.isoformat() + "Z",
            end_time=end_time.isoformat() + "Z",
            duration_ms=duration_ms,
            instance_id=instance_id,
            script_name="custom_script.py",
            exit_code=0,
        )

        shipper = LogShipper(
            opensearch_endpoint=endpoint,
            index_name=index_name,
            region=region,
        )
        shipper.ship(event)
        log.info("LogEvent shipped to OpenSearch successfully.")
    except Exception as exc:  # noqa: BLE001
        # Log shipping failure must never fail the script.
        error_msg = (
            f"{datetime.utcnow().isoformat()}Z "
            f"ERROR: Failed to ship LogEvent to OpenSearch "
            f"(run_id={run_id!r} instance_id={instance_id!r}): {exc}"
        )
        log.error(error_msg)
        _write_fallback_log(error_msg)

    return 0


if __name__ == "__main__":
    sys.exit(main())
