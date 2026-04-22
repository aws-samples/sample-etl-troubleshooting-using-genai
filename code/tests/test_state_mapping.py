"""
Unit tests for dags/state_handlers.py.

Covers:
- handle_glue_state: SUCCEEDED → no exception; FAILED/STOPPED → AirflowException
- handle_lambda_response: 200 → no exception; FunctionError or non-2xx → AirflowException
- handle_ssm_status: "Success" → no exception; "Failed"/"TimedOut" → AirflowException
"""

from __future__ import annotations

import pytest
from airflow.exceptions import AirflowException

from dags.state_handlers import handle_glue_state, handle_lambda_response, handle_ssm_status


# ---------------------------------------------------------------------------
# handle_glue_state
# ---------------------------------------------------------------------------

class TestHandleGlueState:
    def test_succeeded_does_not_raise(self):
        """handle_glue_state('SUCCEEDED') returns None without raising."""
        result = handle_glue_state("SUCCEEDED")
        assert result is None

    def test_failed_raises_airflow_exception(self):
        """handle_glue_state('FAILED') raises AirflowException."""
        with pytest.raises(AirflowException):
            handle_glue_state("FAILED")

    def test_stopped_raises_airflow_exception(self):
        """handle_glue_state('STOPPED') raises AirflowException."""
        with pytest.raises(AirflowException):
            handle_glue_state("STOPPED")

    def test_failed_message_contains_status(self):
        """AirflowException message includes the status string."""
        with pytest.raises(AirflowException, match="FAILED"):
            handle_glue_state("FAILED")

    def test_stopped_message_contains_status(self):
        """AirflowException message includes the status string."""
        with pytest.raises(AirflowException, match="STOPPED"):
            handle_glue_state("STOPPED")


# ---------------------------------------------------------------------------
# handle_lambda_response
# ---------------------------------------------------------------------------

class TestHandleLambdaResponse:
    def test_status_200_no_error_does_not_raise(self):
        """handle_lambda_response({'StatusCode': 200}) returns None without raising."""
        result = handle_lambda_response({"StatusCode": 200})
        assert result is None

    def test_status_200_with_function_error_raises(self):
        """handle_lambda_response with FunctionError set raises AirflowException."""
        with pytest.raises(AirflowException):
            handle_lambda_response({"StatusCode": 200, "FunctionError": "Unhandled"})

    def test_status_500_raises_airflow_exception(self):
        """handle_lambda_response({'StatusCode': 500}) raises AirflowException."""
        with pytest.raises(AirflowException):
            handle_lambda_response({"StatusCode": 500})

    def test_function_error_message_contains_error_type(self):
        """AirflowException message includes the FunctionError value."""
        with pytest.raises(AirflowException, match="Unhandled"):
            handle_lambda_response({"StatusCode": 200, "FunctionError": "Unhandled"})

    def test_non_2xx_message_contains_status_code(self):
        """AirflowException message includes the non-2xx status code."""
        with pytest.raises(AirflowException, match="500"):
            handle_lambda_response({"StatusCode": 500})

    def test_status_202_no_error_does_not_raise(self):
        """handle_lambda_response({'StatusCode': 202}) is also a success."""
        result = handle_lambda_response({"StatusCode": 202})
        assert result is None


# ---------------------------------------------------------------------------
# handle_ssm_status
# ---------------------------------------------------------------------------

class TestHandleSsmStatus:
    def test_success_does_not_raise(self):
        """handle_ssm_status('Success') returns None without raising."""
        result = handle_ssm_status("Success")
        assert result is None

    def test_failed_raises_airflow_exception(self):
        """handle_ssm_status('Failed', stderr=...) raises AirflowException."""
        with pytest.raises(AirflowException):
            handle_ssm_status("Failed", stderr="error output")

    def test_failed_exception_contains_stderr(self):
        """AirflowException message includes the stderr content."""
        with pytest.raises(AirflowException, match="error output"):
            handle_ssm_status("Failed", stderr="error output")

    def test_timed_out_raises_airflow_exception(self):
        """handle_ssm_status('TimedOut') raises AirflowException."""
        with pytest.raises(AirflowException):
            handle_ssm_status("TimedOut")

    def test_failed_no_stderr_raises_airflow_exception(self):
        """handle_ssm_status('Failed') with no stderr still raises AirflowException."""
        with pytest.raises(AirflowException):
            handle_ssm_status("Failed")
