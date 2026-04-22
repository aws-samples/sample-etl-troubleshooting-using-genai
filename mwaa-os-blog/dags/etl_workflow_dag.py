"""
MWAA ETL Workflow DAG

Orchestrates three compute backends in sequence:
  1. AWS Glue job (large-scale extraction/transformation)
  2. AWS Lambda function (lightweight event-driven processing)
  3. EC2 Python script via SSM Run Command (custom persistent-environment processing)

All configurable parameters are read from Airflow Variables so the DAG can be
reconfigured without code changes.  A try/except guard around every Variable.get()
call allows the module to be imported in test environments that have no live
Airflow metastore.

Requirements: 1.1, 1.2, 1.6
"""

import json
import time
from datetime import datetime, timedelta, timezone

import boto3

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)

from dags.callbacks import (
    emit_cloudwatch_metrics,
    log_task_retry,
    log_task_success,
    notify_failure,
    write_summary_log,
)

# ---------------------------------------------------------------------------
# Read configurable parameters from Airflow Variables (with safe defaults)
# ---------------------------------------------------------------------------

try:
    schedule_interval: str = Variable.get("etl_schedule_interval", default_var="@daily")
except Exception:
    schedule_interval = "@daily"

try:
    _retries: int = int(Variable.get("etl_task_retries", default_var=2))
except Exception:
    _retries = 2

try:
    _retry_delay_seconds: int = int(
        Variable.get("etl_task_retry_delay_seconds", default_var=300)
    )
except Exception:
    _retry_delay_seconds = 300

# ---------------------------------------------------------------------------
# Glue task parameters (Task 6.1)
# ---------------------------------------------------------------------------

try:
    _glue_job_name: str = Variable.get("glue_job_name", default_var="etl-extraction-job")
except Exception:
    _glue_job_name = "etl-extraction-job"

try:
    _glue_source_path: str = Variable.get("glue_source_path", default_var="s3://etl-source/")
except Exception:
    _glue_source_path = "s3://etl-source/"

try:
    _glue_target_path: str = Variable.get("glue_target_path", default_var="s3://etl-target/")
except Exception:
    _glue_target_path = "s3://etl-target/"

try:
    _glue_poll_interval: int = int(
        Variable.get("glue_poll_interval_seconds", default_var=30)
    )
except Exception:
    _glue_poll_interval = 30

# ---------------------------------------------------------------------------
# Lambda task parameters (Task 7.1)
# ---------------------------------------------------------------------------

try:
    _lambda_function_name: str = Variable.get(
        "lambda_function_name", default_var="etl-transform"
    )
except Exception:
    _lambda_function_name = "etl-transform"

try:
    _lambda_input_path: str = Variable.get(
        "lambda_input_path", default_var="s3://etl-target/"
    )
except Exception:
    _lambda_input_path = "s3://etl-target/"

# ---------------------------------------------------------------------------
# EC2/SSM task parameters (Task 8.1)
# ---------------------------------------------------------------------------

try:
    _ec2_instance_id: str = Variable.get(
        "ec2_instance_id", default_var="i-0123456789abcdef0"
    )
except Exception:
    _ec2_instance_id = "i-0123456789abcdef0"

try:
    _ec2_input_path: str = Variable.get("ec2_input_path", default_var="s3://etl-target/")
except Exception:
    _ec2_input_path = "s3://etl-target/"

try:
    _ec2_output_path: str = Variable.get("ec2_output_path", default_var="s3://etl-output/")
except Exception:
    _ec2_output_path = "s3://etl-output/"

try:
    _ec2_poll_interval: int = int(
        Variable.get("ec2_poll_interval_seconds", default_var=30)
    )
except Exception:
    _ec2_poll_interval = 30

# ---------------------------------------------------------------------------
# default_args applied to every task in the DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": _retries,
    "retry_delay": timedelta(seconds=_retry_delay_seconds),
    "on_failure_callback": notify_failure,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

dag = DAG(
    dag_id="etl_workflow",
    default_args=default_args,
    # Airflow 2.x used `schedule_interval`; Airflow 3.x renamed it to `schedule`.
    # The variable is still named `schedule_interval` for clarity in the codebase
    # (matching the Airflow Variable key `etl_schedule_interval`).
    schedule=schedule_interval,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    description=(
        "ETL pipeline: Glue extraction → Lambda transform → EC2 custom script. "
        "Supports both scheduled (cron) and manual trigger execution modes."
    ),
    tags=["etl", "mwaa"],
    # DAG-level callbacks: emit metrics and write summary log on success or failure.
    # notify_failure is also included on failure so the DAG-level failure path
    # mirrors the task-level failure path.
    on_success_callback=[emit_cloudwatch_metrics, write_summary_log],
    on_failure_callback=[emit_cloudwatch_metrics, write_summary_log, notify_failure],
)

# ---------------------------------------------------------------------------
# Task 6.1 — Glue extraction task
# ---------------------------------------------------------------------------

glue_extraction = GlueJobOperator(
    task_id="glue_extraction",
    job_name=_glue_job_name,
    script_args={
        "run_id": "{{ run_id }}",
        "source_path": _glue_source_path,
        "target_path": _glue_target_path,
        "partition_date": "{{ ds }}",
    },
    wait_for_completion=True,
    job_poll_interval=_glue_poll_interval,
    aws_conn_id="aws_default",
    on_failure_callback=notify_failure,
    on_success_callback=log_task_success,
    on_retry_callback=log_task_retry,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task 7.1 — Lambda transform task
# ---------------------------------------------------------------------------

lambda_transform = LambdaInvokeFunctionOperator(
    task_id="lambda_transform",
    function_name=_lambda_function_name,
    payload=json.dumps({
        "run_id": "{{ run_id }}",
        "input_path": _lambda_input_path,
    }),
    aws_conn_id="aws_default",
    on_failure_callback=notify_failure,
    on_success_callback=log_task_success,
    on_retry_callback=log_task_retry,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task 8.1 — EC2 custom script via SSM Run Command (PythonOperator wrapper)
# ---------------------------------------------------------------------------

def run_ec2_script(
    run_id: str,
    instance_id: str,
    input_path: str,
    output_path: str,
    poll_interval: int,
) -> str:
    """
    Send an SSM Run Command to the target EC2 instance and poll until
    the command reaches a terminal state.

    Args:
        run_id: The Airflow DAG run ID, passed as an env var to the script.
        instance_id: The EC2 instance ID to target.
        input_path: S3 path for input data.
        output_path: S3 path for output data.
        poll_interval: Seconds to wait between ``GetCommandInvocation`` polls.

    Returns:
        The SSM command invocation ID on success.

    Raises:
        AirflowException: if the command fails or the instance is unreachable.
    """
    ssm = boto3.client("ssm")

    send_response = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={
            "commands": [
                f"export RUN_ID={run_id}",
                f"export INPUT_PATH={input_path}",
                f"export OUTPUT_PATH={output_path}",
                f"export INSTANCE_ID={instance_id}",
                "python3 /opt/etl/custom_script.py",
            ],
            "executionTimeout": ["3600"],
        },
    )

    command_id: str = send_response["Command"]["CommandId"]

    # Poll until the command reaches a terminal state.
    terminal_states = {"Success", "Failed", "TimedOut", "Cancelled", "Undeliverable", "Terminated"}

    while True:
        invocation = ssm.get_command_invocation(
            CommandId=command_id,
            InstanceId=instance_id,
        )
        status_details: str = invocation.get("StatusDetails", "")

        if status_details in terminal_states:
            break

        time.sleep(poll_interval)

    if status_details == "Success":
        return command_id

    stderr: str = invocation.get("StandardErrorContent", "")
    raise AirflowException(
        f"SSM command {command_id!r} failed on instance {instance_id!r} "
        f"with status {status_details!r}: {stderr}"
    )


ec2_custom_script = PythonOperator(
    task_id="ec2_custom_script",
    python_callable=run_ec2_script,
    op_kwargs={
        "run_id": "{{ run_id }}",
        "instance_id": _ec2_instance_id,
        "input_path": _ec2_input_path,
        "output_path": _ec2_output_path,
        "poll_interval": _ec2_poll_interval,
    },
    on_failure_callback=notify_failure,
    on_success_callback=log_task_success,
    on_retry_callback=log_task_retry,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task dependency graph: glue_extraction >> lambda_transform >> ec2_custom_script
# ---------------------------------------------------------------------------

glue_extraction >> lambda_transform >> ec2_custom_script
