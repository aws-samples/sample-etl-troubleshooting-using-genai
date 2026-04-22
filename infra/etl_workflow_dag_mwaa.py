"""
etl_workflow_dag.py — MWAA DAG for the MWAA + OpenSearch Monitoring Blog.

This DAG orchestrates a three-stage ETL pipeline:
  1. AWS Glue job (large-scale data extraction and transformation)
  2. AWS Lambda function (lightweight event-driven processing)
  3. EC2 Python script via SSM Run Command (custom persistent-environment processing)

Every task passes ``run_id`` as a parameter so that all Log_Events from a
single pipeline run share the same correlation key in OpenSearch.

Compatible with Python 3.9+ and apache-airflow-providers-amazon>=6.0.0.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.models import Variable

from dag_callbacks import notify_failure, write_dag_summary

# ---------------------------------------------------------------------------
# DAG default_args
# ---------------------------------------------------------------------------

default_args: Dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

# ---------------------------------------------------------------------------
# EC2 SSM task — uses PythonOperator + boto3 (no SsmRunCommandOperator needed)
# ---------------------------------------------------------------------------

def run_ec2_ssm_command(**context) -> None:
    """Send an SSM Run Command to the EC2 instance and wait for completion."""
    import time

    run_id = context["run_id"]
    instance_id = Variable.get("ec2_instance_id", default_var="")
    region = Variable.get("aws_region", default_var="us-east-1")

    if not instance_id:
        raise ValueError("Airflow Variable 'ec2_instance_id' is not set")

    ssm = boto3.client("ssm", region_name=region)

    response = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={
            "commands": [
                f"export RUN_ID={run_id}",
                "python3 /opt/etl/custom_script.py",
            ],
            "executionTimeout": ["3600"],
        },
    )

    command_id = response["Command"]["CommandId"]

    # Poll until the command completes
    for _ in range(120):  # max 10 minutes
        time.sleep(5)
        result = ssm.get_command_invocation(
            CommandId=command_id,
            InstanceId=instance_id,
        )
        status = result["Status"]
        if status == "Success":
            return
        if status in ("Failed", "Cancelled", "TimedOut", "Undeliverable"):
            raise RuntimeError(
                f"SSM command {command_id} failed with status: {status}\n"
                f"stderr: {result.get('StandardErrorContent', '')}"
            )

    raise TimeoutError(f"SSM command {command_id} did not complete within 10 minutes")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="etl_workflow",
    default_args=default_args,
    description="MWAA-orchestrated ETL pipeline: Glue → Lambda → EC2",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "monitoring", "opensearch"],
    on_success_callback=write_dag_summary,
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Glue extraction job
    # ------------------------------------------------------------------
    glue_extraction_task = GlueJobOperator(
        task_id="glue_extraction",
        job_name="{{ var.value.glue_job_name }}",
        script_args={
            "run_id": "{{ run_id }}",
            "source_path": "{{ var.value.glue_source_path }}",
            "target_path": "{{ var.value.glue_target_path }}",
            "partition_date": "{{ ds }}",
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    # ------------------------------------------------------------------
    # Task 2: Lambda transformation function
    # ------------------------------------------------------------------
    lambda_transform_task = LambdaInvokeFunctionOperator(
        task_id="lambda_transform",
        function_name="{{ var.value.lambda_function_name }}",
        payload=json.dumps(
            {
                "run_id": "{{ run_id }}",
                "input_path": "{{ var.value.lambda_input_path }}",
            }
        ),
        aws_conn_id="aws_default",
    )

    # ------------------------------------------------------------------
    # Task 3: EC2 custom script via SSM (PythonOperator + boto3)
    # ------------------------------------------------------------------
    ec2_custom_task = PythonOperator(
        task_id="ec2_custom_script",
        python_callable=run_ec2_ssm_command,
    )

    # ------------------------------------------------------------------
    # Task dependency graph
    # ------------------------------------------------------------------
    glue_extraction_task >> lambda_transform_task >> ec2_custom_task
