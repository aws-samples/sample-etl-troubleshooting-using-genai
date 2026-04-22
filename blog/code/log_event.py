"""
log_event.py — Shared LogEvent dataclass for the MWAA + OpenSearch monitoring blog.

Represents a single structured log document written to the OpenSearch ``etl-logs-YYYY-MM``
index.  Every pipeline component (MWAA DAG, Glue job, Lambda function, EC2 script) creates
``LogEvent`` instances and forwards them to OpenSearch via the ``LogShipper`` class.

Compatible with Python 3.9+.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_COMPONENT_TYPES = frozenset({"glue", "lambda", "ec2", "mwaa"})
VALID_LOG_LEVELS = frozenset({"INFO", "WARN", "ERROR"})

# Matches ISO 8601 UTC strings such as:
#   2024-03-15T10:30:00Z
#   2024-03-15T10:30:00.123Z
#   2024-03-15T10:30:00+00:00
_ISO8601_UTC_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"  # date + time
    r"(\.\d+)?"                                 # optional fractional seconds
    r"(Z|[+-]00:00)$"                           # UTC offset
)


def _validate_iso8601_utc(value: str, field_name: str) -> None:
    """Raise ``ValueError`` if *value* is not a valid ISO 8601 UTC string."""
    if not isinstance(value, str):
        raise ValueError(
            f"'{field_name}' must be a string, got {type(value).__name__!r}"
        )
    if not _ISO8601_UTC_RE.match(value):
        raise ValueError(
            f"'{field_name}' must be an ISO 8601 UTC string "
            f"(e.g. '2024-03-15T10:30:00Z'), got {value!r}"
        )


def _parse_iso8601(value: str) -> datetime:
    """Parse an ISO 8601 UTC string into a timezone-aware ``datetime``."""
    # Normalise the trailing 'Z' to '+00:00' for fromisoformat (Python 3.9 compat)
    normalised = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalised)


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


@dataclass
class LogEvent:
    """Structured log event document for the ETL pipeline monitoring solution.

    Required base fields
    --------------------
    run_id : str
        MWAA DAG run ID — the correlation key that links every log event from a
        single pipeline run across all compute components.
    task_name : str
        Airflow ``task_id`` that produced this event (e.g. ``"glue_extraction"``).
    component_type : str
        The compute environment that emitted the event.  Must be one of
        ``"glue"``, ``"lambda"``, ``"ec2"``, or ``"mwaa"``.
    log_level : str
        Severity level.  Must be one of ``"INFO"``, ``"WARN"``, or ``"ERROR"``.
    message : str
        Human-readable description of the event.
    timestamp : str
        ISO 8601 UTC string representing when the event occurred
        (e.g. ``"2024-03-15T10:30:00Z"``).

    Component-specific optional fields
    ------------------------------------
    Glue:
        job_name, job_run_id, terminal_status
    Lambda:
        function_name, request_id, outcome, duration_ms
    EC2:
        instance_id, script_name, exit_code
    Shared timing / duration:
        start_time, end_time, duration_ms
    """

    # ------------------------------------------------------------------
    # Required base fields
    # ------------------------------------------------------------------
    run_id: str
    task_name: str
    component_type: str
    log_level: str
    message: str
    timestamp: str

    # ------------------------------------------------------------------
    # Glue-specific optional fields
    # ------------------------------------------------------------------
    job_name: Optional[str] = field(default=None)
    job_run_id: Optional[str] = field(default=None)
    terminal_status: Optional[str] = field(default=None)

    # ------------------------------------------------------------------
    # Lambda-specific optional fields
    # ------------------------------------------------------------------
    function_name: Optional[str] = field(default=None)
    request_id: Optional[str] = field(default=None)
    outcome: Optional[str] = field(default=None)

    # ------------------------------------------------------------------
    # EC2-specific optional fields
    # ------------------------------------------------------------------
    instance_id: Optional[str] = field(default=None)
    script_name: Optional[str] = field(default=None)
    exit_code: Optional[int] = field(default=None)

    # ------------------------------------------------------------------
    # Shared timing / duration fields
    # ------------------------------------------------------------------
    start_time: Optional[str] = field(default=None)
    end_time: Optional[str] = field(default=None)
    duration_ms: Optional[int] = field(default=None)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:  # noqa: C901 (complexity is intentional)
        """Validate all fields after dataclass initialisation."""

        # --- Required string fields must be non-empty strings ---
        required_str_fields = ("run_id", "task_name", "component_type", "log_level", "message", "timestamp")
        for fname in required_str_fields:
            value = getattr(self, fname)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"'{fname}' is required and must be a non-empty string, "
                    f"got {value!r}"
                )

        # --- component_type enumeration ---
        if self.component_type not in VALID_COMPONENT_TYPES:
            raise ValueError(
                f"'component_type' must be one of {sorted(VALID_COMPONENT_TYPES)}, "
                f"got {self.component_type!r}"
            )

        # --- log_level enumeration ---
        if self.log_level not in VALID_LOG_LEVELS:
            raise ValueError(
                f"'log_level' must be one of {sorted(VALID_LOG_LEVELS)}, "
                f"got {self.log_level!r}"
            )

        # --- ISO 8601 UTC validation for timestamp ---
        _validate_iso8601_utc(self.timestamp, "timestamp")

        # --- ISO 8601 UTC validation for optional time fields ---
        if self.start_time is not None:
            _validate_iso8601_utc(self.start_time, "start_time")

        if self.end_time is not None:
            _validate_iso8601_utc(self.end_time, "end_time")

        # --- end_time >= start_time when both are present ---
        if self.start_time is not None and self.end_time is not None:
            start_dt = _parse_iso8601(self.start_time)
            end_dt = _parse_iso8601(self.end_time)
            if end_dt < start_dt:
                raise ValueError(
                    f"'end_time' ({self.end_time!r}) must be >= "
                    f"'start_time' ({self.start_time!r})"
                )

        # --- duration_ms must be a non-negative integer when present ---
        if self.duration_ms is not None:
            if not isinstance(self.duration_ms, int) or self.duration_ms < 0:
                raise ValueError(
                    f"'duration_ms' must be a non-negative integer, "
                    f"got {self.duration_ms!r}"
                )

        # --- exit_code must be an integer when present ---
        if self.exit_code is not None and not isinstance(self.exit_code, int):
            raise ValueError(
                f"'exit_code' must be an integer, got {type(self.exit_code).__name__!r}"
            )

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the ``LogEvent`` to a JSON-compatible ``dict``.

        Only fields with non-``None`` values are included in the output, so
        component-specific optional fields that were not set are omitted from
        the document written to OpenSearch.

        Returns
        -------
        dict
            A flat dictionary whose values are JSON-serialisable primitives
            (``str``, ``int``, ``None`` is excluded).
        """
        result: Dict[str, Any] = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if value is not None:
                result[f.name] = value
        return result
