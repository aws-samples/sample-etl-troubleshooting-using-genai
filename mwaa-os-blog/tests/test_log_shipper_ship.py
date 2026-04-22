"""
Unit tests for LogShipper.ship — retry and exponential backoff behaviour.

Covers:
- Successful delivery on first attempt (no retries)
- Successful delivery after 1, 2, or 3 failures (partial retries)
- All 4 attempts fail → LogShipperError raised with exception chaining
- Correct sleep delays between attempts (0 s, 1 s, 2 s, 4 s)
- Error message written to stderr on final failure
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, call, patch

import pytest

from plugins.log_shipper.exceptions import LogShipperError
from plugins.log_shipper.log_shipper import LogShipper
from plugins.log_shipper.models import LogEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_shipper() -> LogShipper:
    return LogShipper(
        opensearch_endpoint="https://search-test.us-east-1.es.amazonaws.com",
        index_name="etl-logs-2024-01",
        region="us-east-1",
    )


def make_event() -> LogEvent:
    return LogEvent(
        run_id="run-001",
        task_name="glue_extraction",
        component_type="glue",
        log_level="INFO",
        message="Test message.",
        timestamp="2024-01-15T12:00:00Z",
    )


# ---------------------------------------------------------------------------
# Success cases
# ---------------------------------------------------------------------------

class TestShipSuccess:
    def test_succeeds_on_first_attempt(self):
        """ship() returns immediately when _deliver succeeds on the first try."""
        shipper = make_shipper()
        event = make_event()

        with patch.object(shipper, "_deliver") as mock_deliver, \
             patch("time.sleep") as mock_sleep:
            shipper.ship(event)

        mock_deliver.assert_called_once_with(event)
        mock_sleep.assert_not_called()

    @pytest.mark.parametrize("num_failures", [1, 2, 3])
    def test_succeeds_after_partial_failures(self, num_failures):
        """ship() succeeds and stops retrying once _deliver succeeds."""
        shipper = make_shipper()
        event = make_event()

        side_effects = [Exception("transient")] * num_failures + [None]

        with patch.object(shipper, "_deliver", side_effect=side_effects) as mock_deliver, \
             patch("time.sleep"):
            shipper.ship(event)  # should not raise

        assert mock_deliver.call_count == num_failures + 1


# ---------------------------------------------------------------------------
# Failure cases
# ---------------------------------------------------------------------------

class TestShipFailure:
    def test_raises_log_shipper_error_after_all_attempts(self):
        """ship() raises LogShipperError when all 4 attempts fail."""
        shipper = make_shipper()
        event = make_event()

        with patch.object(shipper, "_deliver", side_effect=Exception("persistent error")), \
             patch("time.sleep"), \
             pytest.raises(LogShipperError):
            shipper.ship(event)

    def test_exactly_four_attempts_made(self):
        """ship() calls _deliver exactly 4 times before giving up."""
        shipper = make_shipper()
        event = make_event()

        with patch.object(shipper, "_deliver", side_effect=Exception("fail")) as mock_deliver, \
             patch("time.sleep"):
            with pytest.raises(LogShipperError):
                shipper.ship(event)

        assert mock_deliver.call_count == 4

    def test_exception_is_chained(self):
        """LogShipperError.__cause__ is the original exception."""
        shipper = make_shipper()
        event = make_event()
        original = RuntimeError("root cause")

        with patch.object(shipper, "_deliver", side_effect=original), \
             patch("time.sleep"):
            with pytest.raises(LogShipperError) as exc_info:
                shipper.ship(event)

        assert exc_info.value.__cause__ is original

    def test_stderr_written_on_final_failure(self, capsys):
        """ship() writes an error message to stderr when all attempts fail."""
        shipper = make_shipper()
        event = make_event()

        with patch.object(shipper, "_deliver", side_effect=Exception("boom")), \
             patch("time.sleep"):
            with pytest.raises(LogShipperError):
                shipper.ship(event)

        captured = capsys.readouterr()
        assert "LogShipper" in captured.err
        assert "run-001" in captured.err


# ---------------------------------------------------------------------------
# Backoff timing
# ---------------------------------------------------------------------------

class TestShipBackoff:
    def test_no_sleep_before_first_attempt(self):
        """No sleep occurs before the very first delivery attempt."""
        shipper = make_shipper()
        event = make_event()

        sleep_calls = []

        def fake_deliver(e):
            # Record how many sleeps have happened so far at the time of each call.
            sleep_calls.append(len(mock_sleep.call_args_list))

        with patch.object(shipper, "_deliver", side_effect=fake_deliver), \
             patch("plugins.log_shipper.log_shipper.time.sleep") as mock_sleep:
            shipper.ship(event)

        # First (and only) call to _deliver should have seen 0 sleeps.
        assert sleep_calls[0] == 0

    def test_correct_sleep_delays_on_all_failures(self):
        """Delays between attempts are 1 s, 2 s, 4 s (no delay before attempt 1)."""
        shipper = make_shipper()
        event = make_event()

        with patch.object(shipper, "_deliver", side_effect=Exception("fail")), \
             patch("plugins.log_shipper.log_shipper.time.sleep") as mock_sleep:
            with pytest.raises(LogShipperError):
                shipper.ship(event)

        # time.sleep should be called 3 times (before attempts 2, 3, 4).
        assert mock_sleep.call_count == 3
        assert mock_sleep.call_args_list == [call(1), call(2), call(4)]
