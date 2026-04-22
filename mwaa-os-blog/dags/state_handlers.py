"""
State handlers for the MWAA ETL Workflow.

Each function maps a compute backend's terminal state to either a clean
return (success) or an AirflowException (failure).  These handlers are
called from within the DAG or from operator callbacks to provide a
consistent, testable mapping between backend-specific status strings and
Airflow task outcomes.

Requirements: 2.3, 2.4, 3.2, 3.3, 4.3, 4.4
"""

from __future__ import annotations

from airflow.exceptions import AirflowException


# ---------------------------------------------------------------------------
# Task 6.3 — Glue terminal-state handler
# ---------------------------------------------------------------------------

def handle_glue_state(status: str) -> None:
    """
    Map a Glue job terminal state to an Airflow task outcome.

    Args:
        status: The Glue job run status string returned by ``GetJobRun``.
                Expected values: ``"SUCCEEDED"``, ``"FAILED"``, ``"STOPPED"``.

    Returns:
        None on success (``status == "SUCCEEDED"``).

    Raises:
        AirflowException: if ``status`` is ``"FAILED"`` or ``"STOPPED"``.

    Requirements: 2.3, 2.4
    """
    if status == "SUCCEEDED":
        return
    if status in ("FAILED", "STOPPED"):
        raise AirflowException(f"Glue job reached terminal state: {status}")
    # Unknown / intermediate states are not terminal — callers should not
    # invoke this handler until the job has reached a terminal state.
    raise AirflowException(f"Glue job reached unexpected state: {status}")


# ---------------------------------------------------------------------------
# Task 7.3 — Lambda response handler
# ---------------------------------------------------------------------------

def handle_lambda_response(response: dict) -> None:
    """
    Inspect a Lambda invocation response and raise on error conditions.

    Args:
        response: The dict returned by ``boto3.client('lambda').invoke()``.
                  Relevant keys: ``FunctionError``, ``StatusCode``.

    Returns:
        None when the invocation succeeded (no ``FunctionError`` and
        ``StatusCode`` in 200–299).

    Raises:
        AirflowException: if ``FunctionError`` is set in the response.
        AirflowException: if ``StatusCode`` is outside the 200–299 range.

    Requirements: 3.2, 3.3
    """
    if response.get("FunctionError"):
        raise AirflowException(
            f"Lambda function error: {response['FunctionError']}"
        )

    status_code = response.get("StatusCode", 0)
    if status_code not in range(200, 300):
        raise AirflowException(
            f"Lambda returned non-2xx status: {response.get('StatusCode')}"
        )


# ---------------------------------------------------------------------------
# Task 8.3 — EC2/SSM terminal-state handler
# ---------------------------------------------------------------------------

def handle_ssm_status(status_details: str, stderr: str = "") -> None:
    """
    Map an SSM command invocation status to an Airflow task outcome.

    Args:
        status_details: The ``StatusDetails`` string from
                        ``GetCommandInvocation``.  The success value is
                        ``"Success"``; all other values are treated as
                        failures.
        stderr: The ``StandardErrorContent`` captured from the command
                invocation.  Included in the exception message on failure.

    Returns:
        None when ``status_details == "Success"``.

    Raises:
        AirflowException: for any non-``"Success"`` status.

    Requirements: 4.3, 4.4
    """
    if status_details == "Success":
        return
    raise AirflowException(
        f"SSM command failed with status {status_details!r}: {stderr}"
    )
