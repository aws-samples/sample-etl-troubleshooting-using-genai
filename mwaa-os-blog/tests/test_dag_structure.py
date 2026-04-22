"""
Unit tests for DAG structure in dags/etl_workflow_dag.py.

Verifies:
- DAG imports without errors (smoke test)
- DAG has dag_id="etl_workflow"
- DAG has exactly the expected task IDs
- Task dependency order: glue_extraction → lambda_transform → ec2_custom_script
- Each task has retries configured (not None)
- Each task has retry_delay configured (not None)
- on_failure_callback is attached to each task (not None)
- DAG has catchup=False
- DAG has max_active_runs=1

Requirements: 1.1, 1.4, 1.6
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dag():
    """Import and return the etl_workflow DAG object."""
    from dags.etl_workflow_dag import dag as _dag
    return _dag


@pytest.fixture(scope="module")
def tasks(dag):
    """Return a dict of task_id → task object for the DAG."""
    return {task.task_id: task for task in dag.tasks}


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------

class TestDagImport:
    def test_dag_imports_without_error(self):
        """Importing dags.etl_workflow_dag must not raise any exception."""
        import dags.etl_workflow_dag  # noqa: F401


# ---------------------------------------------------------------------------
# DAG-level attributes
# ---------------------------------------------------------------------------

class TestDagAttributes:
    def test_dag_id(self, dag):
        """DAG must have dag_id='etl_workflow'."""
        assert dag.dag_id == "etl_workflow"

    def test_catchup_false(self, dag):
        """DAG must have catchup=False."""
        assert dag.catchup is False

    def test_max_active_runs(self, dag):
        """DAG must have max_active_runs=1."""
        assert dag.max_active_runs == 1


# ---------------------------------------------------------------------------
# Task IDs
# ---------------------------------------------------------------------------

EXPECTED_TASK_IDS = {"glue_extraction", "lambda_transform", "ec2_custom_script"}


class TestTaskIds:
    def test_expected_task_ids_present(self, tasks):
        """DAG must contain exactly the three expected task IDs."""
        assert set(tasks.keys()) == EXPECTED_TASK_IDS

    def test_no_extra_tasks(self, tasks):
        """DAG must not contain any task IDs beyond the expected set."""
        extra = set(tasks.keys()) - EXPECTED_TASK_IDS
        assert extra == set(), f"Unexpected task IDs found: {extra}"


# ---------------------------------------------------------------------------
# Task dependency order
# ---------------------------------------------------------------------------

class TestTaskDependencies:
    def test_glue_extraction_downstream_is_lambda_transform(self, tasks):
        """glue_extraction must have lambda_transform as a downstream task."""
        glue = tasks["glue_extraction"]
        assert "lambda_transform" in glue.downstream_task_ids

    def test_lambda_transform_downstream_is_ec2_custom_script(self, tasks):
        """lambda_transform must have ec2_custom_script as a downstream task."""
        lam = tasks["lambda_transform"]
        assert "ec2_custom_script" in lam.downstream_task_ids

    def test_glue_extraction_has_no_upstream(self, tasks):
        """glue_extraction must have no upstream tasks (it is the first task)."""
        glue = tasks["glue_extraction"]
        assert len(glue.upstream_task_ids) == 0

    def test_ec2_custom_script_has_no_downstream(self, tasks):
        """ec2_custom_script must have no downstream tasks (it is the last task)."""
        ec2 = tasks["ec2_custom_script"]
        assert len(ec2.downstream_task_ids) == 0


# ---------------------------------------------------------------------------
# Retry configuration
# ---------------------------------------------------------------------------

class TestRetryConfiguration:
    @pytest.mark.parametrize("task_id", sorted(EXPECTED_TASK_IDS))
    def test_task_has_retries(self, tasks, task_id):
        """Each task must have retries configured (not None)."""
        task = tasks[task_id]
        assert task.retries is not None, f"Task {task_id!r} has retries=None"

    @pytest.mark.parametrize("task_id", sorted(EXPECTED_TASK_IDS))
    def test_task_has_retry_delay(self, tasks, task_id):
        """Each task must have retry_delay configured (not None)."""
        task = tasks[task_id]
        assert task.retry_delay is not None, f"Task {task_id!r} has retry_delay=None"


# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------

class TestFailureCallback:
    @pytest.mark.parametrize("task_id", sorted(EXPECTED_TASK_IDS))
    def test_task_has_on_failure_callback(self, tasks, task_id):
        """Each task must have on_failure_callback attached (not None)."""
        task = tasks[task_id]
        assert task.on_failure_callback is not None, (
            f"Task {task_id!r} has on_failure_callback=None"
        )
