"""
Unit tests for DSL query and alerting configuration files.

Validates Requirements 7.1, 8.1, 10.2:
- All four DSL query JSON files are valid JSON with the expected structure.
- alert_monitor.json is valid JSON and contains a `triggers` array.
"""

import json
import pathlib
import pytest

CONFIG_DIR = pathlib.Path(__file__).parent.parent / "config"


def load_json(filename: str) -> dict:
    """Load and parse a JSON file from the config directory."""
    path = CONFIG_DIR / filename
    with open(path) as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# dsl_query_by_run_id.json
# ---------------------------------------------------------------------------

class TestDslQueryByRunId:
    def test_valid_json(self):
        data = load_json("dsl_query_by_run_id.json")
        assert isinstance(data, dict)

    def test_has_query_term_run_id(self):
        data = load_json("dsl_query_by_run_id.json")
        assert "query" in data
        assert "term" in data["query"]
        assert "run_id" in data["query"]["term"]

    def test_sorted_ascending(self):
        data = load_json("dsl_query_by_run_id.json")
        assert "sort" in data
        sort_fields = {list(s.keys())[0]: list(s.values())[0] for s in data["sort"]}
        assert sort_fields["timestamp"]["order"] == "asc"

    def test_has_size(self):
        data = load_json("dsl_query_by_run_id.json")
        assert "size" in data
        assert data["size"] > 0

    def test_placeholder_run_id_is_realistic(self):
        data = load_json("dsl_query_by_run_id.json")
        run_id = data["query"]["term"]["run_id"]
        assert run_id.startswith("scheduled__"), (
            "Placeholder run_id should use the MWAA scheduled__ prefix"
        )


# ---------------------------------------------------------------------------
# dsl_query_errors.json
# ---------------------------------------------------------------------------

class TestDslQueryErrors:
    def test_valid_json(self):
        data = load_json("dsl_query_errors.json")
        assert isinstance(data, dict)

    def test_filters_on_log_level_error(self):
        data = load_json("dsl_query_errors.json")
        filters = data["query"]["bool"]["filter"]
        term_filters = [f for f in filters if "term" in f]
        assert any(
            f["term"].get("log_level") == "ERROR" for f in term_filters
        ), "Query must filter for log_level=ERROR"

    def test_has_timestamp_range_filter(self):
        data = load_json("dsl_query_errors.json")
        filters = data["query"]["bool"]["filter"]
        range_filters = [f for f in filters if "range" in f]
        assert len(range_filters) >= 1
        assert "timestamp" in range_filters[0]["range"]

    def test_sorted_descending(self):
        data = load_json("dsl_query_errors.json")
        assert "sort" in data
        sort_fields = {list(s.keys())[0]: list(s.values())[0] for s in data["sort"]}
        assert sort_fields["timestamp"]["order"] == "desc"


# ---------------------------------------------------------------------------
# dsl_query_duration_agg.json
# ---------------------------------------------------------------------------

class TestDslQueryDurationAgg:
    def test_valid_json(self):
        data = load_json("dsl_query_duration_agg.json")
        assert isinstance(data, dict)

    def test_size_zero(self):
        """Aggregation-only queries must set size=0 to suppress hits."""
        data = load_json("dsl_query_duration_agg.json")
        assert data.get("size") == 0

    def test_has_terms_agg_on_component_type(self):
        data = load_json("dsl_query_duration_agg.json")
        by_component = data["aggs"]["by_component"]
        assert by_component["terms"]["field"] == "component_type"

    def test_has_avg_duration_sub_agg(self):
        data = load_json("dsl_query_duration_agg.json")
        sub_aggs = data["aggs"]["by_component"]["aggs"]
        assert "avg_duration_ms" in sub_aggs
        assert sub_aggs["avg_duration_ms"]["avg"]["field"] == "duration_ms"


# ---------------------------------------------------------------------------
# dsl_query_p95_duration.json
# ---------------------------------------------------------------------------

class TestDslQueryP95Duration:
    def test_valid_json(self):
        data = load_json("dsl_query_p95_duration.json")
        assert isinstance(data, dict)

    def test_size_zero(self):
        data = load_json("dsl_query_p95_duration.json")
        assert data.get("size") == 0

    def test_has_terms_agg_on_component_type(self):
        data = load_json("dsl_query_p95_duration.json")
        by_component = data["aggs"]["by_component"]
        assert by_component["terms"]["field"] == "component_type"

    def test_has_percentiles_sub_agg_with_p95(self):
        data = load_json("dsl_query_p95_duration.json")
        sub_aggs = data["aggs"]["by_component"]["aggs"]
        assert "p95_duration_ms" in sub_aggs
        pct_agg = sub_aggs["p95_duration_ms"]["percentiles"]
        assert pct_agg["field"] == "duration_ms"
        assert 95 in pct_agg["percents"]


# ---------------------------------------------------------------------------
# alert_monitor.json
# ---------------------------------------------------------------------------

class TestAlertMonitor:
    def test_valid_json(self):
        data = load_json("alert_monitor.json")
        assert isinstance(data, dict)

    def test_has_triggers_array(self):
        """Requirements 8.1: monitor must include a triggers array."""
        data = load_json("alert_monitor.json")
        assert "triggers" in data
        assert isinstance(data["triggers"], list)
        assert len(data["triggers"]) >= 1

    def test_trigger_has_condition(self):
        data = load_json("alert_monitor.json")
        trigger = data["triggers"][0]
        assert "condition" in trigger
        assert "script" in trigger["condition"]

    def test_trigger_has_actions(self):
        data = load_json("alert_monitor.json")
        trigger = data["triggers"][0]
        assert "actions" in trigger
        assert len(trigger["actions"]) >= 1

    def test_has_sns_destination(self):
        """Requirements 8.2: destination must point to an SNS topic ARN."""
        data = load_json("alert_monitor.json")
        assert "destinations" in data
        sns_destinations = [
            d for d in data["destinations"] if d.get("type") == "sns"
        ]
        assert len(sns_destinations) >= 1

    def test_sns_topic_arn_placeholder_format(self):
        data = load_json("alert_monitor.json")
        sns_dest = next(d for d in data["destinations"] if d.get("type") == "sns")
        topic_arn = sns_dest["sns"]["topic_arn"]
        assert topic_arn.startswith("arn:aws:sns:"), (
            "SNS topic ARN must use the arn:aws:sns: prefix"
        )

    def test_monitor_type_is_monitor(self):
        data = load_json("alert_monitor.json")
        assert data.get("type") == "monitor"

    def test_monitor_has_schedule(self):
        data = load_json("alert_monitor.json")
        assert "schedule" in data
        assert "period" in data["schedule"]

    def test_monitor_queries_error_log_level(self):
        """The monitor input query must filter for log_level=ERROR."""
        data = load_json("alert_monitor.json")
        search_query = data["inputs"][0]["search"]["query"]
        filters = search_query["query"]["bool"]["filter"]
        term_filters = [f for f in filters if "term" in f]
        assert any(
            f["term"].get("log_level") == "ERROR" for f in term_filters
        ), "Alert monitor query must filter for log_level=ERROR"

    def test_monitor_uses_etl_logs_index_pattern(self):
        data = load_json("alert_monitor.json")
        indices = data["inputs"][0]["search"]["indices"]
        assert any("etl-logs" in idx for idx in indices)
