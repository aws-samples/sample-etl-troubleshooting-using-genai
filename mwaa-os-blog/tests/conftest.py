"""
pytest configuration and shared fixtures for the MWAA ETL Workflow test suite.
"""

import functools
import sys
import types

import pytest

# ---------------------------------------------------------------------------
# Compatibility shims for apache-airflow-providers-amazon 8.x on Airflow 3.x
#
# Airflow 3.x moved / removed several internal modules that the providers
# package still imports.  Inject lightweight stubs so the provider can be
# imported in the test environment without a full Airflow installation.
# ---------------------------------------------------------------------------

_COMPAT_STUBS: dict[str, dict] = {
    "airflow.utils.log.secrets_masker": {
        "mask_secret": lambda x: None,
        "redact": lambda x, y=None: x,
        "should_hide_sensitive_variable_fields_in_ui": lambda: False,
    },
    "airflow.compat": {},
    "airflow.compat.functools": {"cache": functools.cache},
}

for _mod_name, _attrs in _COMPAT_STUBS.items():
    if _mod_name not in sys.modules:
        _mod = types.ModuleType(_mod_name)
        for _k, _v in _attrs.items():
            setattr(_mod, _k, _v)
        sys.modules[_mod_name] = _mod


# ---------------------------------------------------------------------------
# Hypothesis settings
# ---------------------------------------------------------------------------
# Register a named settings profile so individual test modules can opt in to
# a faster "ci" profile without changing the default behaviour.
try:
    from hypothesis import settings, HealthCheck

    settings.register_profile(
        "ci",
        max_examples=100,
        suppress_health_check=[HealthCheck.too_slow],
    )
    settings.register_profile(
        "dev",
        max_examples=20,
    )
    # Use the "ci" profile by default; override with HYPOTHESIS_PROFILE env var.
    settings.load_profile("ci")
except ImportError:
    pass  # hypothesis is optional at import time; tests that need it will fail explicitly


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_log_event():
    """Return a minimal valid LogEvent instance for use in unit tests."""
    from plugins.log_shipper.models import LogEvent

    return LogEvent(
        run_id="run-001",
        task_name="glue_extraction",
        component_type="glue",
        log_level="INFO",
        message="Glue job completed successfully.",
        timestamp="2024-01-15T12:00:00Z",
        start_time="2024-01-15T11:55:00Z",
        end_time="2024-01-15T12:00:00Z",
        duration_ms=300_000,
        job_name="etl-extraction-job",
        job_run_id="jr_abc123",
        terminal_status="SUCCEEDED",
    )
