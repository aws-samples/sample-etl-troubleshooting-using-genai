"""
test_log_event.py — Unit tests for the LogEvent dataclass.

Validates: Requirements 3.1, 3.6
"""

import json
import pytest

from blog.code.log_event import LogEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = {
    "run_id": "scheduled__2024-03-15T10:00:00+00:00",
    "task_name": "glue_extraction",
    "component_type": "glue",
    "log_level": "INFO",
    "message": "Glue job completed successfully",
    "timestamp": "2024-03-15T10:28:45Z",
}


def make_event(**overrides) -> LogEvent:
    """Return a valid LogEvent, optionally overriding fields."""
    kwargs = {**REQUIRED_FIELDS, **overrides}
    return LogEvent(**kwargs)


# ---------------------------------------------------------------------------
# 1. Serialisation — to_dict() produces a JSON-compatible dict
# ---------------------------------------------------------------------------


class TestToDictSerialization:
    def test_required_fields_present_in_dict(self):
        event = make_event()
        d = event.to_dict()
        for key, value in REQUIRED_FIELDS.items():
            assert d[key] == value

    def test_dict_is_json_serialisable(self):
        event = make_event(
            job_name="etl-extraction-job",
            job_run_id="jr_abc123",
            terminal_status="SUCCEEDED",
            start_time="2024-03-15T10:00:12Z",
            end_time="2024-03-15T10:28:45Z",
            duration_ms=1713000,
        )
        d = event.to_dict()
        # Should not raise
        serialised = json.dumps(d)
        assert isinstance(serialised, str)

    def test_none_optional_fields_excluded_from_dict(self):
        event = make_event()
        d = event.to_dict()
        optional_fields = [
            "job_name", "job_run_id", "terminal_status",
            "function_name", "request_id", "outcome",
            "instance_id", "script_name", "exit_code",
            "start_time", "end_time", "duration_ms",
        ]
        for f in optional_fields:
            assert f not in d, f"Expected {f!r} to be absent when None"

    def test_optional_fields_included_when_set(self):
        event = make_event(
            component_type="lambda",
            function_name="etl-transform-function",
            request_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            outcome="success",
            duration_ms=16000,
        )
        d = event.to_dict()
        assert d["function_name"] == "etl-transform-function"
        assert d["request_id"] == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        assert d["outcome"] == "success"
        assert d["duration_ms"] == 16000

    def test_exit_code_zero_included_in_dict(self):
        """exit_code=0 is a valid value and must not be omitted."""
        event = make_event(component_type="ec2", exit_code=0)
        d = event.to_dict()
        assert "exit_code" in d
        assert d["exit_code"] == 0

    def test_all_component_types_serialise(self):
        for ct in ("glue", "lambda", "ec2", "mwaa"):
            event = make_event(component_type=ct)
            d = event.to_dict()
            assert d["component_type"] == ct


# ---------------------------------------------------------------------------
# 2. Required-field validation — missing or empty raises ValueError
# ---------------------------------------------------------------------------


class TestRequiredFieldValidation:
    @pytest.mark.parametrize("missing_field", [
        "run_id", "task_name", "component_type", "log_level", "message", "timestamp",
    ])
    def test_missing_required_field_raises_value_error(self, missing_field):
        kwargs = {k: v for k, v in REQUIRED_FIELDS.items() if k != missing_field}
        with pytest.raises((ValueError, TypeError)):
            LogEvent(**kwargs)

    @pytest.mark.parametrize("empty_field", [
        "run_id", "task_name", "message",
    ])
    def test_empty_string_required_field_raises_value_error(self, empty_field):
        with pytest.raises(ValueError):
            make_event(**{empty_field: ""})

    @pytest.mark.parametrize("whitespace_field", [
        "run_id", "task_name", "message",
    ])
    def test_whitespace_only_required_field_raises_value_error(self, whitespace_field):
        with pytest.raises(ValueError):
            make_event(**{whitespace_field: "   "})

    def test_invalid_component_type_raises_value_error(self):
        with pytest.raises(ValueError, match="component_type"):
            make_event(component_type="kafka")

    def test_invalid_log_level_raises_value_error(self):
        with pytest.raises(ValueError, match="log_level"):
            make_event(log_level="DEBUG")

    def test_valid_log_levels_accepted(self):
        for level in ("INFO", "WARN", "ERROR"):
            event = make_event(log_level=level)
            assert event.log_level == level


# ---------------------------------------------------------------------------
# 3. ISO 8601 UTC timestamp validation
# ---------------------------------------------------------------------------


class TestTimestampValidation:
    @pytest.mark.parametrize("valid_ts", [
        "2024-03-15T10:30:00Z",
        "2024-03-15T10:30:00.123Z",
        "2024-03-15T10:30:00+00:00",
        "2024-03-15T00:00:00Z",
    ])
    def test_valid_iso8601_utc_timestamps_accepted(self, valid_ts):
        event = make_event(timestamp=valid_ts)
        assert event.timestamp == valid_ts

    @pytest.mark.parametrize("invalid_ts", [
        "2024-03-15",                        # date only
        "10:30:00",                          # time only
        "2024-03-15 10:30:00",              # space separator
        "2024-03-15T10:30:00+05:30",        # non-UTC offset
        "not-a-timestamp",
        "",
        "2024-03-15T10:30:00",              # missing timezone
    ])
    def test_invalid_timestamp_raises_value_error(self, invalid_ts):
        with pytest.raises(ValueError):
            make_event(timestamp=invalid_ts)

    def test_valid_start_time_accepted(self):
        event = make_event(start_time="2024-03-15T10:00:00Z")
        assert event.start_time == "2024-03-15T10:00:00Z"

    def test_invalid_start_time_raises_value_error(self):
        with pytest.raises(ValueError):
            make_event(start_time="2024-03-15")

    def test_valid_end_time_accepted(self):
        event = make_event(
            start_time="2024-03-15T10:00:00Z",
            end_time="2024-03-15T10:30:00Z",
        )
        assert event.end_time == "2024-03-15T10:30:00Z"

    def test_invalid_end_time_raises_value_error(self):
        with pytest.raises(ValueError):
            make_event(end_time="March 15 2024")


# ---------------------------------------------------------------------------
# 4. end_time >= start_time enforcement
# ---------------------------------------------------------------------------


class TestEndTimeStartTimeOrdering:
    def test_end_time_equal_to_start_time_is_valid(self):
        event = make_event(
            start_time="2024-03-15T10:00:00Z",
            end_time="2024-03-15T10:00:00Z",
        )
        assert event.start_time == event.end_time

    def test_end_time_after_start_time_is_valid(self):
        event = make_event(
            start_time="2024-03-15T10:00:00Z",
            end_time="2024-03-15T10:30:00Z",
        )
        assert event.end_time > event.start_time

    def test_end_time_before_start_time_raises_value_error(self):
        with pytest.raises(ValueError, match="end_time"):
            make_event(
                start_time="2024-03-15T10:30:00Z",
                end_time="2024-03-15T10:00:00Z",
            )

    def test_only_start_time_present_is_valid(self):
        event = make_event(start_time="2024-03-15T10:00:00Z")
        assert event.start_time is not None
        assert event.end_time is None

    def test_only_end_time_present_is_valid(self):
        event = make_event(end_time="2024-03-15T10:30:00Z")
        assert event.end_time is not None
        assert event.start_time is None

    def test_end_time_before_start_time_with_fractional_seconds(self):
        with pytest.raises(ValueError, match="end_time"):
            make_event(
                start_time="2024-03-15T10:30:00.500Z",
                end_time="2024-03-15T10:30:00.000Z",
            )


# ---------------------------------------------------------------------------
# 5. Component-specific field round-trips
# ---------------------------------------------------------------------------


class TestComponentSpecificFields:
    def test_glue_fields_round_trip(self):
        event = make_event(
            component_type="glue",
            job_name="etl-extraction-job",
            job_run_id="jr_abc123def456",
            terminal_status="SUCCEEDED",
            start_time="2024-03-15T10:00:12Z",
            end_time="2024-03-15T10:28:45Z",
            duration_ms=1713000,
        )
        d = event.to_dict()
        assert d["job_name"] == "etl-extraction-job"
        assert d["job_run_id"] == "jr_abc123def456"
        assert d["terminal_status"] == "SUCCEEDED"
        assert d["duration_ms"] == 1713000

    def test_lambda_fields_round_trip(self):
        event = make_event(
            component_type="lambda",
            function_name="etl-transform-function",
            request_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            outcome="success",
            start_time="2024-03-15T10:28:47Z",
            end_time="2024-03-15T10:29:03Z",
            duration_ms=16000,
        )
        d = event.to_dict()
        assert d["function_name"] == "etl-transform-function"
        assert d["outcome"] == "success"
        assert d["duration_ms"] == 16000

    def test_ec2_fields_round_trip(self):
        event = make_event(
            component_type="ec2",
            instance_id="i-0abc123def456789",
            script_name="custom_script.py",
            exit_code=0,
            start_time="2024-03-15T10:29:05Z",
            end_time="2024-03-15T10:45:22Z",
            duration_ms=977000,
        )
        d = event.to_dict()
        assert d["instance_id"] == "i-0abc123def456789"
        assert d["script_name"] == "custom_script.py"
        assert d["exit_code"] == 0

    def test_mwaa_fields_round_trip(self):
        event = make_event(
            component_type="mwaa",
            task_name="dag_summary",
            start_time="2024-03-15T10:00:00Z",
            end_time="2024-03-15T10:45:30Z",
            duration_ms=2730000,
        )
        d = event.to_dict()
        assert d["component_type"] == "mwaa"
        assert d["duration_ms"] == 2730000
