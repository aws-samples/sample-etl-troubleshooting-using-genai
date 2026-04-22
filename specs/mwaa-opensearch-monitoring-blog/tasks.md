# Implementation Plan: MWAA + OpenSearch Monitoring Blog

## Overview

This plan breaks the blog post into discrete coding tasks that produce all required artifacts: the blog's Python code samples, JSON configuration files, and their accompanying unit/integration tests. Tasks follow the narrative arc of the blog — schema → shipper → index config → DAG integration → queries → alerting → security → reproducibility — so each step builds directly on the previous one.

All Python code targets Python 3.9+. All JSON artifacts must be valid and directly applicable via the AWS CLI or OpenSearch REST API.

## Tasks

- [x] 1. Create project structure and shared data model
  - Create the directory layout: `blog/code/`, `blog/config/`, `blog/tests/`
  - Implement `blog/code/log_event.py`: `LogEvent` dataclass with all required base fields (`run_id`, `task_name`, `component_type`, `log_level`, `message`, `timestamp`) and component-specific optional fields (`job_name`, `job_run_id`, `terminal_status`, `function_name`, `request_id`, `outcome`, `instance_id`, `script_name`, `exit_code`, `start_time`, `end_time`, `duration_ms`)
  - Implement `to_dict()` method that serializes the dataclass to a JSON-compatible dict
  - Validate required fields in `__post_init__`; raise `ValueError` for missing or invalid values
  - Validate `timestamp`, `start_time`, `end_time` as ISO 8601 UTC strings; enforce `end_time >= start_time` when both are present
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.6, 10.1_

  - [ ]* 1.1 Write unit tests for `LogEvent`
    - Test that `LogEvent` with all required fields serializes to a valid JSON-compatible dict
    - Test that missing required fields raise `ValueError`
    - Test that `timestamp`, `start_time`, `end_time` are validated as ISO 8601 UTC strings
    - Test that `end_time >= start_time` is enforced
    - _Requirements: 3.1, 3.6_

- [x] 2. Implement the `LogShipper` class
  - Implement `blog/code/log_shipper.py`: `LogShipper` class with `__init__(opensearch_endpoint, index_name, region)` and `ship(event: LogEvent) -> None`
  - Use `boto3` to obtain AWS credentials and sign requests with SigV4 via `requests-aws4auth`; no username/password credentials
  - Implement retry logic: up to 3 retries (4 total attempts) with exponential backoff delays of 1 s, 2 s, 4 s; on final failure raise `LogShipperError` and emit a local error log to stderr
  - Implement `_get_endpoint_from_secrets_manager(secret_name: str) -> str` class method that retrieves the OpenSearch endpoint from AWS Secrets Manager at runtime
  - Ensure the `ship()` call-site in each compute environment (Glue, Lambda, EC2, MWAA) wraps the call in `try/except` so a log shipping failure does not crash the compute job
  - _Requirements: 4.1, 4.2, 4.3, 4.6, 9.2, 9.3_

  - [ ]* 2.1 Write unit tests for `LogShipper`
    - Test that `ship()` sends an HTTP POST to the correct OpenSearch endpoint
    - Test that the request includes a SigV4 `Authorization` header (contains `AWS4-HMAC-SHA256`)
    - Test that the request does not include an `Authorization: Basic` header
    - Test that when `requests.post` raises an exception once, `ship()` retries and succeeds on the second attempt
    - Test that when `requests.post` raises an exception 4 times, `ship()` raises `LogShipperError` after exactly 4 attempts
    - Test that retry delays follow the 1 s / 2 s / 4 s exponential backoff pattern
    - _Requirements: 4.2, 4.3_

- [x] 3. Write call-site examples for each compute environment
  - Implement `blog/code/glue_job_example.py`: complete Glue job script snippet showing `LogShipper` instantiation, `LogEvent` construction with Glue-specific fields (`job_name`, `job_run_id`, `terminal_status`), and `ship()` call wrapped in `try/except`
  - Implement `blog/code/lambda_handler_example.py`: complete Lambda handler snippet showing `LogShipper` usage with Lambda-specific fields (`function_name`, `request_id`, `outcome`, `duration_ms`)
  - Implement `blog/code/ec2_script_example.py`: complete EC2 script snippet showing `LogShipper` usage with EC2-specific fields (`instance_id`, `script_name`, `exit_code`)
  - Each snippet must include all required `import` statements and be compatible with Python 3.9+
  - _Requirements: 4.4, 4.5, 10.1_

- [x] 4. Write OpenSearch index configuration files
  - Create `blog/config/index_mapping.json`: complete index mapping with correct field types — `keyword` for `run_id`, `task_name`, `component_type`, `log_level`, and all identifier fields; `text` for `message`; `date` for `timestamp`, `start_time`, `end_time`; `long` for `duration_ms`; `integer` for `exit_code`
  - Create `blog/config/ism_policy.json`: complete ISM policy with `hot` → `delete` transition at `min_index_age: 30d`, `ism_template` matching `etl-logs-*` with priority 100
  - Add inline comments (as a companion README or doc-string) explaining why `keyword` vs `text` is used for each field
  - _Requirements: 5.1, 5.2, 5.3, 5.5_

  - [ ]* 4.1 Write unit tests for JSON config files
    - Test that `index_mapping.json` is valid JSON and contains a `mappings.properties` key
    - Test that `ism_policy.json` is valid JSON and contains a `policy.states` array with a `delete` state
    - _Requirements: 5.1, 5.3, 10.2_

- [x] 5. Write MWAA DAG integration code
  - Implement `blog/code/etl_workflow_dag.py` (excerpt): DAG definition with `on_failure_callback` (`notify_failure`) that publishes to SNS and writes a `Log_Event` to OpenSearch containing `run_id`, `task_name`, `failure_reason`, and `timestamp`
  - Implement DAG-level `on_success_callback` that writes a summary `Log_Event` containing `run_id`, `overall_status`, `total_duration`, and per-task status dict
  - Show how `{{ run_id }}` is passed as a parameter to each downstream task (Glue `script_args`, Lambda `payload`, EC2 SSM command environment variables)
  - Wrap all `LogShipper` calls in the callbacks with `try/except` that logs to the Airflow task log rather than re-raising
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

  - [ ]* 5.1 Write unit tests for DAG callbacks
    - Test that `notify_failure` publishes to SNS with the correct message structure (`run_id`, `task_name`, `failure_reason`, `timestamp`)
    - Test that the summary callback invokes `LogShipper.ship()` with a summary event containing `run_id`, `overall_status`, `total_duration`, and `task_statuses`
    - Test that callback functions do not raise import errors
    - _Requirements: 6.2, 6.3_

- [x] 6. Checkpoint — validate all code artifacts
  - Ensure all Python files import without errors
  - Ensure all JSON files parse with `json.loads`
  - Ensure all unit tests pass
  - Ask the user if any questions arise before proceeding to query and alerting artifacts.

- [x] 7. Write DSL query and alerting configuration files
  - Create `blog/config/dsl_query_by_run_id.json`: DSL query returning all events for a specific `run_id`, sorted by `timestamp` ascending
  - Create `blog/config/dsl_query_errors.json`: DSL query filtering for `log_level: ERROR` within a configurable time range, sorted by `timestamp` descending
  - Create `blog/config/dsl_query_duration_agg.json`: DSL aggregation query computing average `duration_ms` by `component_type`
  - Create `blog/config/dsl_query_p95_duration.json`: DSL aggregation query computing 95th-percentile `duration_ms` by `component_type`
  - Create `blog/config/alert_monitor.json`: OpenSearch alerting monitor config that triggers when the count of `log_level: ERROR` events exceeds a configurable threshold within a rolling time window; include a `triggers` array and a destination pointing to an SNS topic ARN placeholder
  - _Requirements: 7.1, 7.5, 8.1, 8.2_

  - [ ]* 7.1 Write unit tests for DSL queries and alert config
    - Test that all four DSL query JSON files are valid JSON
    - Test that `alert_monitor.json` is valid JSON and contains a `triggers` array
    - _Requirements: 7.1, 8.1, 10.2_

  - [ ]* 7.2 Write integration tests for DSL queries (requires local OpenSearch)
    - Seed a local OpenSearch instance (via Docker) with sample `Log_Event` documents covering all four component types
    - Test that Query 1 returns only documents matching the specified `run_id`
    - Test that Query 2 returns only documents with `log_level: ERROR` within the time range
    - Test that Query 3 returns an aggregation with one bucket per `component_type`
    - Test that Query 4 returns a percentile aggregation with a `95.0` key in each bucket
    - _Requirements: 7.1, 7.5_

- [x] 8. Write IAM policy configuration file
  - Create `blog/config/iam_policy_opensearch.json`: least-privilege IAM policy granting only `es:ESHttpPost` on a specific OpenSearch index ARN (not `*`); include a placeholder ARN in the format `arn:aws:es:REGION:ACCOUNT_ID:domain/DOMAIN_NAME/etl-logs-*`
  - Add a companion section in the blog explaining the four IAM roles (`MWAAExecutionRole`, `GlueServiceRole`, `LambdaExecutionRole`, `EC2InstanceProfile`) and the OpenSearch permissions each needs
  - _Requirements: 9.1, 9.4, 5.6_

  - [ ]* 8.1 Write unit test for IAM policy file
    - Test that `iam_policy_opensearch.json` is valid JSON and does not contain `"Resource": "*"`
    - _Requirements: 9.1, 10.2_

- [x] 9. Assemble the blog post Markdown file
  - Create `blog/blog_post.md` with all twelve sections in order: Introduction, Architecture Overview, Prerequisites, Log_Event Schema Design, Log_Shipper Implementation, OpenSearch Index Configuration, MWAA DAG Integration, Dashboards and Queries, Alerting and Anomaly Detection, Security Best Practices, Reproducibility Reference, Conclusion
  - Embed the architecture Mermaid diagram in Section 2
  - Embed all code artifacts as fenced code blocks with correct language tags (`python`, `json`, `bash`)
  - Include the four example `Log_Event` JSON documents (Glue, Lambda, EC2, MWAA) in Section 4
  - Include the troubleshooting table (three issues: events not appearing, SigV4 errors, mapping conflicts) in Section 11
  - List all pinned package dependencies (`requests-aws4auth==1.3.1`, `boto3>=1.34.0`, `apache-airflow-providers-amazon>=8.0.0`) in Section 11
  - Include AWS CLI commands to create the OpenSearch index with the mapping and apply the ISM policy in Section 6
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.1–2.5, 3.1–3.6, 4.1–4.6, 5.1–5.6, 6.1–6.5, 7.1–7.5, 8.1–8.4, 9.1–9.5, 10.1–10.6_

- [x] 10. Run content review checklist
  - Verify every acceptance criterion in `requirements.md` is addressed by at least one section or code sample in `blog_post.md`
  - Verify all code samples include `import` statements and are compatible with Python 3.9+
  - Verify all JSON documents are valid (parseable by `json.loads`)
  - Verify all field names in code samples match the `Log_Event` schema exactly
  - Verify all IAM policy ARNs use specific resource ARNs, not `*`
  - Verify the `run_id` correlation key is explained and demonstrated in every relevant section
  - Verify the troubleshooting section addresses all three required issues
  - Verify package versions are pinned
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP
- The integration tests in task 7.2 require a local OpenSearch instance (Docker); they can be deferred if Docker is not available
- All code artifacts in `blog/code/` are the canonical source of truth; the blog post embeds them verbatim
- The companion spec (`mwaa-etl-workflow`) is the authoritative reference for all schemas, field names, and IAM role names — any discrepancy must be resolved in favor of that spec
