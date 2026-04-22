"""
Unit tests for LogEvent.validate() and LogEvent.to_dict().

Covers:
- validate() raises ValueError when any required field is missing or empty
- validate() raises ValueError when component_type is invalid
- validate() raises ValueError when log_level is invalid
- validate() raises ValueError when end_time < start_time
- validate() passes for a fully valid event
- to_dict() returns only non-None fields
- to_dict() result round-trips through json.dumps / json.loads without error
"""

from __future__ import annotations

import json

import pytest

from plugins.log_shipper.models import LogEvent, VALID_COMPONENT_TYPES, VALID_LOG_LEVELS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_valid_event(**overrides) -> LogEvent:
    """Return a minimal valid LogEvent, with optional field overrides."""
    defaults = dict(
        run_id="run-001",
        task_name="glue_extraction",
        component_type="glue",
        log_level="INFO",
        message="All good.",
        timestamp="2024-01-15T12:00:00Z",
    )
    defaults.update(overrides)
    return LogEvent(**defaults)


# ---------------------------------------------------------------------------
# validate() — required field checks
# ---------------------------------------------------------------------------

class TestValidateRequiredFields:
    REQUIRED_FIELDS = ["run_id", "task_name", "component_type", "log_level", "message", "timestamp"]

    @pytest.mark.parametrize("field", REQUIRED_FIELDS)
    def test_raises_when_field_is_none(self, field):
        """validate() raises ValueError when a required field is None."""
        event = make_valid_event(**{field: None})
        with pytest.raises(ValueError, match=field):
            event.validate()

    @pytest.mark.parametrize("field", REQUIRED_FIELDS)
    def test_raises_when_field_is_empty_string(self, field):
        """validate() raises ValueError when a required field is an empty string."""
        event = make_valid_event(**{field: ""})
        with pytest.raises(ValueError, match=field):
            event.validate()

    def test_passes_for_fully_valid_event(self):
        """validate() does not raise for a fully valid event."""
        event = make_valid_event(
            start_time="2024-01-15T11:55:00Z",
            end_time="2024-01-15T12:00:00Z",
            duration_ms=300_000,
            job_name="etl-extraction-job",
            job_run_id="jr_abc123",
            terminal_status="SUCCEEDED",
        )
        event.validate()  # should not raise


# ---------------------------------------------------------------------------
# validate() — component_type validation
# ---------------------------------------------------------------------------

class TestValidateComponentType:
    def test_raises_for_invalid_component_type(self):
        """validate() raises ValueError when component_type is not in the allowed set."""
        event = make_valid_event(component_type="kafka")
        with pytest.raises(ValueError, match="component_type"):
            event.validate()

    def test_raises_for_uppercase_component_type(self):
        """validate() raises ValueError for 'GLUE' (case-sensitive)."""
        event = make_valid_event(component_type="GLUE")
        with pytest.raises(ValueError, match="component_type"):
            event.validate()

    @pytest.mark.parametrize("ct", sorted(VALID_COMPONENT_TYPES))
    def test_passes_for_all_valid_component_types(self, ct):
        """validate() accepts every valid component_type value."""
        event = make_valid_event(component_type=ct)
        event.validate()  # should not raise


# ---------------------------------------------------------------------------
# validate() — log_level validation
# ---------------------------------------------------------------------------

class TestValidateLogLevel:
    def test_raises_for_invalid_log_level(self):
        """validate() raises ValueError when log_level is not in the allowed set."""
        event = make_valid_event(log_level="DEBUG")
        with pytest.raises(ValueError, match="log_level"):
            event.validate()

    def test_raises_for_lowercase_log_level(self):
        """validate() raises ValueError for 'info' (case-sensitive)."""
        event = make_valid_event(log_level="info")
        with pytest.raises(ValueError, match="log_level"):
            event.validate()

    @pytest.mark.parametrize("level", sorted(VALID_LOG_LEVELS))
    def test_passes_for_all_valid_log_levels(self, level):
        """validate() accepts every valid log_level value."""
        event = make_valid_event(log_level=level)
        event.validate()  # should not raise


# ---------------------------------------------------------------------------
# validate() — temporal consistency
# ---------------------------------------------------------------------------

class TestValidateTemporalConsistency:
    def test_raises_when_end_time_before_start_time(self):
        """validate() raises ValueError when end_time < start_time."""
        event = make_valid_event(
            start_time="2024-01-15T12:00:00Z",
            end_time="2024-01-15T11:00:00Z",
        )
        with pytest.raises(ValueError, match="end_time"):
            event.validate()

    def test_passes_when_end_time_equals_start_time(self):
        """validate() accepts end_time == start_time (zero-duration task)."""
        event = make_valid_event(
            start_time="2024-01-15T12:00:00Z",
            end_time="2024-01-15T12:00:00Z",
        )
        event.validate()  # should not raise

    def test_passes_when_end_time_after_start_time(self):
        """validate() accepts end_time > start_time."""
        event = make_valid_event(
            start_time="2024-01-15T11:55:00Z",
            end_time="2024-01-15T12:00:00Z",
        )
        event.validate()  # should not raise

    def test_passes_when_only_start_time_set(self):
        """validate() does not raise when only start_time is provided (end_time is None)."""
        event = make_valid_event(start_time="2024-01-15T12:00:00Z")
        event.validate()  # should not raise

    def test_passes_when_only_end_time_set(self):
        """validate() does not raise when only end_time is provided (start_time is None)."""
        event = make_valid_event(end_time="2024-01-15T12:00:00Z")
        event.validate()  # should not raise

    def test_passes_when_neither_time_set(self):
        """validate() does not raise when both start_time and end_time are None."""
        event = make_valid_event()
        event.validate()  # should not raise


# ---------------------------------------------------------------------------
# to_dict() — only non-None fields
# ---------------------------------------------------------------------------

class TestToDict:
    def test_required_fields_present(self):
        """to_dict() includes all required base fields."""
        event = make_valid_event()
        d = event.to_dict()
        for field in ["run_id", "task_name", "component_type", "log_level", "message", "timestamp"]:
            assert field in d

    def test_none_fields_excluded(self):
        """to_dict() omits fields whose value is None."""
        event = make_valid_event()
        # All optional fields are None by default on a minimal event.
        d = event.to_dict()
        optional_fields = [
            "start_time", "end_time", "duration_ms",
            "job_name", "job_run_id", "terminal_status",
            "function_name", "request_id", "outcome",
            "instance_id", "script_name", "exit_code",
        ]
        for field in optional_fields:
            assert field not in d, f"Expected {field!r} to be absent when None"

    def test_optional_fields_included_when_set(self):
        """to_dict() includes optional fields that have non-None values."""
        event = make_valid_event(
            start_time="2024-01-15T11:55:00Z",
            end_time="2024-01-15T12:00:00Z",
            duration_ms=300_000,
            job_name="etl-extraction-job",
        )
        d = event.to_dict()
        assert d["start_time"] == "2024-01-15T11:55:00Z"
        assert d["end_time"] == "2024-01-15T12:00:00Z"
        assert d["duration_ms"] == 300_000
        assert d["job_name"] == "etl-extraction-job"

    def test_json_round_trip_minimal(self):
        """to_dict() result round-trips through json.dumps / json.loads without error."""
        event = make_valid_event()
        d = event.to_dict()
        serialized = json.dumps(d)
        deserialized = json.loads(serialized)
        assert deserialized == d

    def test_json_round_trip_full(self):
        """to_dict() round-trips correctly for a fully populated event."""
        event = LogEvent(
            run_id="run-999",
            task_name="lambda_transform",
            component_type="lambda",
            log_level="ERROR",
            message="Lambda invocation failed.",
            timestamp="2024-01-15T13:00:00Z",
            start_time="2024-01-15T12:59:00Z",
            end_time="2024-01-15T13:00:00Z",
            duration_ms=60_000,
            function_name="etl-transform",
            request_id="req-xyz",
            outcome="error",
        )
        d = event.to_dict()
        assert json.loads(json.dumps(d)) == d

    def test_exit_code_zero_included(self):
        """to_dict() includes exit_code=0 (falsy but not None)."""
        event = make_valid_event(component_type="ec2", exit_code=0)
        d = event.to_dict()
        assert "exit_code" in d
        assert d["exit_code"] == 0
