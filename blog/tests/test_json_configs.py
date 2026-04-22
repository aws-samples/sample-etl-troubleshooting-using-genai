"""
Unit tests for JSON configuration files.

Validates Requirements 9.1, 10.2:
- iam_policy_opensearch.json is valid JSON and does not contain "Resource": "*"
- index_mapping.json is valid JSON and contains a mappings.properties key
- ism_policy.json is valid JSON and contains a policy.states array with a delete state
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


def raw_text(filename: str) -> str:
    """Return the raw text of a config file."""
    path = CONFIG_DIR / filename
    return path.read_text()


# ---------------------------------------------------------------------------
# iam_policy_opensearch.json
# ---------------------------------------------------------------------------

class TestIamPolicyOpenSearch:
    """Validates Requirements 9.1, 10.2."""

    def test_valid_json(self):
        """File must be parseable as valid JSON."""
        data = load_json("iam_policy_opensearch.json")
        assert isinstance(data, dict)

    def test_no_wildcard_resource(self):
        """
        Requirements 9.1: least-privilege policy must NOT use "Resource": "*".
        Every statement must reference a specific ARN.
        """
        data = load_json("iam_policy_opensearch.json")
        for statement in data.get("Statement", []):
            resource = statement.get("Resource", "")
            if isinstance(resource, list):
                assert "*" not in resource, (
                    'IAM policy must not contain "Resource": "*" — use a specific ARN'
                )
            else:
                assert resource != "*", (
                    'IAM policy must not contain "Resource": "*" — use a specific ARN'
                )

    def test_resource_uses_opensearch_arn_format(self):
        """Resource ARN must reference an OpenSearch (es) domain, not a wildcard service."""
        data = load_json("iam_policy_opensearch.json")
        for statement in data.get("Statement", []):
            resource = statement.get("Resource", "")
            resources = resource if isinstance(resource, list) else [resource]
            for arn in resources:
                assert arn.startswith("arn:aws:es:"), (
                    f"Resource ARN '{arn}' must start with 'arn:aws:es:' for OpenSearch"
                )

    def test_resource_targets_etl_logs_index(self):
        """Resource ARN must scope down to the etl-logs-* index pattern."""
        data = load_json("iam_policy_opensearch.json")
        for statement in data.get("Statement", []):
            resource = statement.get("Resource", "")
            resources = resource if isinstance(resource, list) else [resource]
            for arn in resources:
                assert "etl-logs-" in arn, (
                    f"Resource ARN '{arn}' must target the etl-logs-* index pattern"
                )

    def test_action_is_es_http_post_only(self):
        """
        Requirements 9.1: least-privilege — only es:ESHttpPost should be granted,
        not broader permissions like es:* or es:ESHttp*.
        """
        data = load_json("iam_policy_opensearch.json")
        for statement in data.get("Statement", []):
            actions = statement.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            assert "es:ESHttpPost" in actions, (
                "Policy must grant es:ESHttpPost"
            )
            for action in actions:
                assert action == "es:ESHttpPost", (
                    f"Policy must grant ONLY es:ESHttpPost, found '{action}'"
                )

    def test_effect_is_allow(self):
        """The statement effect must be Allow."""
        data = load_json("iam_policy_opensearch.json")
        for statement in data.get("Statement", []):
            assert statement.get("Effect") == "Allow"

    def test_has_version(self):
        """IAM policy must include a Version field."""
        data = load_json("iam_policy_opensearch.json")
        assert "Version" in data
        assert data["Version"] == "2012-10-17"

    def test_raw_text_does_not_contain_resource_star(self):
        """
        Belt-and-suspenders check: the raw file text must not contain
        the literal string '"Resource": "*"' in any form.
        """
        text = raw_text("iam_policy_opensearch.json")
        assert '"Resource": "*"' not in text, (
            'Raw policy text must not contain "Resource": "*"'
        )
        # Also check without spaces around colon
        assert '"Resource":"*"' not in text, (
            'Raw policy text must not contain "Resource":"*"'
        )


# ---------------------------------------------------------------------------
# index_mapping.json  (re-validated here for completeness per design doc)
# ---------------------------------------------------------------------------

class TestIndexMapping:
    """Validates Requirements 5.1, 10.2."""

    def test_valid_json(self):
        data = load_json("index_mapping.json")
        assert isinstance(data, dict)

    def test_has_mappings_properties(self):
        data = load_json("index_mapping.json")
        assert "mappings" in data
        assert "properties" in data["mappings"]


# ---------------------------------------------------------------------------
# ism_policy.json  (re-validated here for completeness per design doc)
# ---------------------------------------------------------------------------

class TestIsmPolicy:
    """Validates Requirements 5.3, 10.2."""

    def test_valid_json(self):
        data = load_json("ism_policy.json")
        assert isinstance(data, dict)

    def test_has_policy_states_with_delete(self):
        data = load_json("ism_policy.json")
        assert "policy" in data
        states = data["policy"].get("states", [])
        assert isinstance(states, list)
        state_names = [s.get("name") for s in states]
        assert "delete" in state_names, (
            "ISM policy must contain a 'delete' state for retention enforcement"
        )
