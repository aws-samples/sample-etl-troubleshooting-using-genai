"""
etl_workflow_dag.py — MWAA DAG for the MWAA + OpenSearch Monitoring Blog.

This DAG orchestrates a three-stage ETL pipeline:
  1. AWS Glue job (large-scale data extraction and transformation)
  2. AWS Lambda function (lightweight event-driven processing)
  3. EC2 Python script via SSM Run Command (custom persistent-environment processing)

Every task passes ``{{ run_id }}`` as a parameter so that all Log_Events from a
single pipeline run share the same correlation key in OpenSearch.

Callbacks are defined in ``blog.code.dag_callbacks`` and imported here so they
can be tested independently of the Airflow operator classes.

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5

Compatible with Python 3.9+ and ``apache-airflow-providers-amazon>=8.0.0``.

Dependencies
------------
- apache-airflow>=2.6.0
- apache-airflow-providers-amazon>=8.0.0
- boto3>=1.34.0
- requests>=2.31.0
- requests-aws4auth==1.3.1
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.operators.ssm import SsmRunCommandOperator

# Callbacks are in a separate module so they can be unit-tested without
# importing the Airflow operator classes (which require a full MWAA environment).
from blog.code.dag_callbacks import notify_failure, write_dag_summary

# ---------------------------------------------------------------------------
# DAG default_args
# ---------------------------------------------------------------------------

default_args: Dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1, tzinfo=timezone.utc),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Attach notify_failure to every task so that any task failure triggers
    # both an SNS notification and an OpenSearch Log_Event.
    "on_failure_callback": notify_failure,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="etl_workflow",
    default_args=default_args,
    description="MWAA-orchestrated ETL pipeline: Glue → Lambda → EC2",
    schedule_interval=None,          # Manual trigger; set a cron string for scheduled runs
    catchup=False,
    max_active_runs=1,
    tags=["etl", "monitoring", "opensearch"],
    # DAG-level success callback writes the summary Log_Event to OpenSearch.
    on_success_callback=write_dag_summary,
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Glue extraction job
    #
    # ``{{ run_id }}`` is passed via ``script_args`` so the Glue job script
    # can include it in every Log_Event it ships to OpenSearch.
    # ------------------------------------------------------------------
    glue_extraction_task = GlueJobOperator(
        task_id="glue_extraction",
        job_name="{{ var.value.glue_job_name }}",
        script_args={
            # Correlation key — every Log_Event from this Glue run will carry
            # this run_id so it can be joined with MWAA events in OpenSearch.
            "run_id": "{{ run_id }}",
            "source_path": "{{ var.value.glue_source_path }}",
            "target_path": "{{ var.value.glue_target_path }}",
            "partition_date": "{{ ds }}",
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
        poll_interval=30,
    )

    # ------------------------------------------------------------------
    # Task 2: Lambda transformation function
    #
    # ``{{ run_id }}`` is embedded in the JSON payload so the Lambda handler
    # can include it in its Log_Event.
    # ------------------------------------------------------------------
    lambda_transform_task = LambdaInvokeFunctionOperator(
        task_id="lambda_transform",
        function_name="{{ var.value.lambda_function_name }}",
        # json.dumps is called at DAG parse time; the Jinja template inside
        # the string is rendered by Airflow at execution time.
        payload=json.dumps(
            {
                "run_id": "{{ run_id }}",
                "input_path": "{{ var.value.lambda_input_path }}",
            }
        ),
        aws_conn_id="aws_default",
    )

    # ------------------------------------------------------------------
    # Task 3: EC2 custom script via SSM Run Command
    #
    # ``{{ run_id }}`` is exported as an environment variable (RUN_ID) so
    # the EC2 Python script can read it and include it in its Log_Event.
    # ------------------------------------------------------------------
    ec2_custom_task = SsmRunCommandOperator(
        task_id="ec2_custom_script",
        instance_ids=["{{ var.value.ec2_instance_id }}"],
        document_name="AWS-RunShellScript",
        parameters={
            "commands": [
                # Export run_id and paths as environment variables so the
                # EC2 script can include run_id in every Log_Event it ships.
                "export RUN_ID={{ run_id }}",
                "export INPUT_PATH={{ var.value.ec2_input_path }}",
                "export OUTPUT_PATH={{ var.value.ec2_output_path }}",
                "python3 /opt/etl/custom_script.py",
            ],
            "executionTimeout": ["3600"],
        },
        aws_conn_id="aws_default",
        poll_interval=30,
    )

    # ------------------------------------------------------------------
    # Task dependency graph: linear pipeline
    #   glue_extraction >> lambda_transform >> ec2_custom_script
    # ------------------------------------------------------------------
    glue_extraction_task >> lambda_transform_task >> ec2_custom_task
