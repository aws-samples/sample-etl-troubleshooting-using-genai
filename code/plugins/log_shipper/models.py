"""
Data models for the MWAA ETL Workflow log shipper.

Defines the LogEvent dataclass that represents a structured log document
written to the central OpenSearch index.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional


# Valid values for component_type and log_level fields
VALID_COMPONENT_TYPES = frozenset({"glue", "lambda", "ec2", "mwaa"})
VALID_LOG_LEVELS = frozenset({"INFO", "WARN", "ERROR"})


@dataclass
class LogEvent:
    """
    Structured log event written to the OpenSearch index.

    Required base fields (must be present and non-None for every event):
        run_id, task_name, component_type, log_level, message, timestamp

    Optional timing fields:
        start_time, end_time, duration_ms

    Component-specific optional fields:
        Glue:   job_name, job_run_id, terminal_status
        Lambda: function_name, request_id, outcome
        EC2:    instance_id, script_name, exit_code
    """

    # --- Required base fields ---
    run_id: str
    task_name: str
    component_type: str          # one of: glue, lambda, ec2, mwaa
    log_level: str               # one of: INFO, WARN, ERROR
    message: str
    timestamp: str               # ISO 8601 UTC, e.g. "2024-01-15T12:00:00Z"

    # --- Optional timing fields ---
    start_time: Optional[str] = None    # ISO 8601 UTC
    end_time: Optional[str] = None      # ISO 8601 UTC
    duration_ms: Optional[int] = None

    # --- Glue-specific optional fields ---
    job_name: Optional[str] = None
    job_run_id: Optional[str] = None
    terminal_status: Optional[str] = None   # SUCCEEDED / FAILED / STOPPED

    # --- Lambda-specific optional fields ---
    function_name: Optional[str] = None
    request_id: Optional[str] = None
    outcome: Optional[str] = None           # "success" or "error"

    # --- EC2-specific optional fields ---
    instance_id: Optional[str] = None
    script_name: Optional[str] = None
    exit_code: Optional[int] = None

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> None:
        """
        Validate the event against the Log_Event schema rules.

        Raises:
            ValueError: if any required base field is missing/None, if
                        component_type or log_level hold an invalid value,
                        or if end_time < start_time when both are present.
        """
        required_fields = ("run_id", "task_name", "component_type", "log_level", "message", "timestamp")
        for fname in required_fields:
            value = getattr(self, fname)
            if value is None or value == "":
                raise ValueError(f"LogEvent.{fname} is required and must not be empty.")

        if self.component_type not in VALID_COMPONENT_TYPES:
            raise ValueError(
                f"LogEvent.component_type must be one of {sorted(VALID_COMPONENT_TYPES)}, "
                f"got {self.component_type!r}."
            )

        if self.log_level not in VALID_LOG_LEVELS:
            raise ValueError(
                f"LogEvent.log_level must be one of {sorted(VALID_LOG_LEVELS)}, "
                f"got {self.log_level!r}."
            )

        if self.start_time is not None and self.end_time is not None:
            if self.end_time < self.start_time:
                raise ValueError(
                    f"LogEvent.end_time ({self.end_time!r}) must be >= start_time ({self.start_time!r})."
                )

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """
        Return a JSON-serializable dict representation of this event.

        Only fields with non-None values are included so that the
        OpenSearch document stays compact.
        """
        return {k: v for k, v in asdict(self).items() if v is not None}
