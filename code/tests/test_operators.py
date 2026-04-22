"""
Unit tests for operator-level configuration in dags/etl_workflow_dag.py.

Verifies:
- glue_extraction is a GlueJobOperator with wait_for_completion=True
- glue_extraction script_args contains required keys
- lambda_transform is a LambdaInvokeFunctionOperator
- lambda_transform payload (parsed from JSON) contains run_id key
- ec2_custom_script is a PythonOperator
- ec2_custom_script op_kwargs contains required keys

Requirements: 2.1, 3.1, 4.1
"""

from __future__ import annotations

import json

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def tasks():
    """Return a dict of task_id → task object for the etl_workflow DAG."""
    from dags.etl_workflow_dag import dag
    return {task.task_id: task for task in dag.tasks}


# ---------------------------------------------------------------------------
# glue_extraction
# ---------------------------------------------------------------------------

class TestGlueExtractionOperator:
    def test_is_glue_job_operator(self, tasks):
        """glue_extraction must be a GlueJobOperator instance."""
        from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
        assert isinstance(tasks["glue_extraction"], GlueJobOperator)

    def test_wait_for_completion_true(self, tasks):
        """glue_extraction must have wait_for_completion=True."""
        task = tasks["glue_extraction"]
        assert task.wait_for_completion is True

    def test_script_args_contains_run_id(self, tasks):
        """glue_extraction script_args must contain the 'run_id' key."""
        task = tasks["glue_extraction"]
        assert "run_id" in task.script_args

    def test_script_args_contains_source_path(self, tasks):
        """glue_extraction script_args must contain the 'source_path' key."""
        task = tasks["glue_extraction"]
        assert "source_path" in task.script_args

    def test_script_args_contains_target_path(self, tasks):
        """glue_extraction script_args must contain the 'target_path' key."""
        task = tasks["glue_extraction"]
        assert "target_path" in task.script_args

    def test_script_args_contains_partition_date(self, tasks):
        """glue_extraction script_args must contain the 'partition_date' key."""
        task = tasks["glue_extraction"]
        assert "partition_date" in task.script_args


# ---------------------------------------------------------------------------
# lambda_transform
# ---------------------------------------------------------------------------

class TestLambdaTransformOperator:
    def test_is_lambda_invoke_function_operator(self, tasks):
        """lambda_transform must be a LambdaInvokeFunctionOperator instance."""
        from airflow.providers.amazon.aws.operators.lambda_function import (
            LambdaInvokeFunctionOperator,
        )
        assert isinstance(tasks["lambda_transform"], LambdaInvokeFunctionOperator)

    def test_payload_contains_run_id(self, tasks):
        """lambda_transform payload (parsed from JSON) must contain the 'run_id' key."""
        task = tasks["lambda_transform"]
        payload = json.loads(task.payload)
        assert "run_id" in payload, (
            f"'run_id' not found in lambda_transform payload keys: {list(payload.keys())}"
        )


# ---------------------------------------------------------------------------
# ec2_custom_script
# ---------------------------------------------------------------------------

class TestEc2CustomScriptOperator:
    def test_is_python_operator(self, tasks):
        """ec2_custom_script must be a PythonOperator instance."""
        from airflow.operators.python import PythonOperator
        assert isinstance(tasks["ec2_custom_script"], PythonOperator)

    def test_op_kwargs_contains_run_id(self, tasks):
        """ec2_custom_script op_kwargs must contain the 'run_id' key."""
        task = tasks["ec2_custom_script"]
        assert "run_id" in task.op_kwargs

    def test_op_kwargs_contains_instance_id(self, tasks):
        """ec2_custom_script op_kwargs must contain the 'instance_id' key."""
        task = tasks["ec2_custom_script"]
        assert "instance_id" in task.op_kwargs

    def test_op_kwargs_contains_input_path(self, tasks):
        """ec2_custom_script op_kwargs must contain the 'input_path' key."""
        task = tasks["ec2_custom_script"]
        assert "input_path" in task.op_kwargs

    def test_op_kwargs_contains_output_path(self, tasks):
        """ec2_custom_script op_kwargs must contain the 'output_path' key."""
        task = tasks["ec2_custom_script"]
        assert "output_path" in task.op_kwargs

    def test_op_kwargs_contains_poll_interval(self, tasks):
        """ec2_custom_script op_kwargs must contain the 'poll_interval' key."""
        task = tasks["ec2_custom_script"]
        assert "poll_interval" in task.op_kwargs
