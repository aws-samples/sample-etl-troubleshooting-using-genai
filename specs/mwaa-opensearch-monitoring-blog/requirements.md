# Requirements Document

## Introduction

This feature is a technical blog post that teaches readers how to monitor Amazon Managed Workflows for Apache Airflow (MWAA) and the downstream workflows it orchestrates — specifically AWS Glue jobs, AWS Lambda functions, and EC2-hosted Python scripts — using Amazon OpenSearch Service. The blog walks through a reference architecture where every pipeline component emits structured JSON log events to a central OpenSearch index, and shows readers how to build dashboards, run queries, and set up alerts to gain full observability over their MWAA-driven ETL pipelines.

The blog is grounded in the concrete MWAA ETL workflow described in the companion spec (`mwaa-etl-workflow`), which uses a single Airflow DAG to coordinate Glue, Lambda, and EC2 tasks, each shipping `Log_Event` documents to OpenSearch via IAM/SigV4 authentication.

## Glossary

- **Blog**: The technical blog post artifact produced by this spec.
- **Reader**: A developer, data engineer, or platform engineer who reads the Blog and follows its guidance.
- **MWAA**: Amazon Managed Workflows for Apache Airflow — the managed orchestration service that schedules and monitors the ETL DAG.
- **DAG**: Directed Acyclic Graph — the Airflow workflow definition that describes task dependencies and execution order.
- **ETL_Workflow**: The end-to-end pipeline managed by MWAA, encompassing Glue, Lambda, and EC2 tasks.
- **Glue_Job**: An AWS Glue job responsible for large-scale data extraction and transformation.
- **Lambda_Function**: An AWS Lambda function responsible for lightweight, event-driven processing steps.
- **EC2_Script**: A Python script executed on an Amazon EC2 instance for custom processing.
- **Log_Shipper**: The shared Python library running on each compute resource that forwards structured log events to OpenSearch.
- **Log_Event**: A structured JSON document written to OpenSearch representing a single log entry from any pipeline component.
- **OpenSearch_Index**: An Amazon OpenSearch Service index that stores and makes searchable all Log_Events from the ETL pipeline.
- **OpenSearch_Dashboard**: A visualization built in OpenSearch Dashboards that displays pipeline health, task durations, error rates, and other metrics derived from Log_Events.
- **ISM_Policy**: An OpenSearch Index State Management policy that automates index lifecycle actions such as retention and deletion.
- **SigV4**: AWS Signature Version 4 — the request-signing protocol used to authenticate to OpenSearch without username/password credentials.
- **Run_ID**: A unique identifier assigned by MWAA to each DAG execution instance, used to correlate all Log_Events from a single pipeline run.

---

## Requirements

### Requirement 1: Blog Structure and Narrative Arc

**User Story:** As a Reader, I want the blog to follow a clear, logical structure, so that I can understand the monitoring architecture and reproduce it in my own environment.

#### Acceptance Criteria

1. THE Blog SHALL include an introduction section that explains the problem being solved: the difficulty of monitoring distributed ETL pipelines that span MWAA, Glue, Lambda, and EC2.
2. THE Blog SHALL include an architecture overview section that describes the end-to-end monitoring flow from pipeline component to OpenSearch.
3. THE Blog SHALL include a prerequisites section that lists the AWS services, IAM permissions, and software versions a Reader needs before following the guide.
4. THE Blog SHALL include step-by-step implementation sections covering: Log_Event schema design, Log_Shipper setup, OpenSearch index configuration, and OpenSearch_Dashboard creation.
5. THE Blog SHALL include a conclusion section that summarizes what was built and suggests next steps (e.g., alerting, anomaly detection).
6. WHEN the Blog references a code sample, THE Blog SHALL provide a complete, runnable code snippet rather than a partial or pseudocode fragment.

---

### Requirement 2: Architecture Overview

**User Story:** As a Reader, I want a clear architecture diagram and explanation, so that I understand how MWAA, Glue, Lambda, EC2, and OpenSearch fit together in the monitoring solution.

#### Acceptance Criteria

1. THE Blog SHALL include an architecture diagram showing the flow of Log_Events from each pipeline component (MWAA DAG, Glue_Job, Lambda_Function, EC2_Script) to the OpenSearch_Index.
2. THE Blog SHALL explain the role of the Log_Shipper as the shared component responsible for delivering Log_Events from each compute environment to OpenSearch.
3. THE Blog SHALL explain why IAM-based authentication (SigV4) is used instead of username/password credentials when writing to OpenSearch.
4. THE Blog SHALL describe the `run_id` field as the correlation key that links all Log_Events from a single DAG run across all pipeline components.
5. THE Blog SHALL explain the OpenSearch index naming convention (e.g., `etl-logs-YYYY-MM`) and the ISM_Policy that enforces 30-day retention.

---

### Requirement 3: Log_Event Schema Design

**User Story:** As a Reader, I want to understand the Log_Event schema, so that I can design consistent structured logs for my own pipeline components.

#### Acceptance Criteria

1. THE Blog SHALL present the complete Log_Event JSON schema, including all required base fields: `run_id`, `task_name`, `component_type`, `log_level`, `message`, and `timestamp`.
2. THE Blog SHALL explain the purpose of each required field and provide an example value for each.
3. THE Blog SHALL describe the component-specific optional fields for each compute type: `job_name` and `job_run_id` for Glue; `function_name`, `request_id`, `outcome`, and `duration_ms` for Lambda; `instance_id`, `script_name`, and `exit_code` for EC2.
4. THE Blog SHALL explain the `log_level` enumeration (`INFO`, `WARN`, `ERROR`) and when each level should be used.
5. THE Blog SHALL provide at least one complete example Log_Event JSON document for each component type (Glue, Lambda, EC2, MWAA).
6. THE Blog SHALL explain the importance of using ISO 8601 UTC format for the `timestamp`, `start_time`, and `end_time` fields to enable accurate time-range queries in OpenSearch.

---

### Requirement 4: Log_Shipper Implementation

**User Story:** As a Reader, I want working code for the Log_Shipper library, so that I can deploy it to my Glue jobs, Lambda functions, EC2 instances, and MWAA environment.

#### Acceptance Criteria

1. THE Blog SHALL provide the complete Python source code for the `LogShipper` class, including the `ship(event: LogEvent)` method.
2. THE Blog SHALL show how to authenticate to OpenSearch using `boto3` credentials and the `requests-aws4auth` library for SigV4 signing.
3. THE Blog SHALL explain and demonstrate the retry logic: up to 3 retries with exponential backoff (delays of 1 s, 2 s, 4 s), followed by a local error log on final failure.
4. THE Blog SHALL show how to deploy the Log_Shipper to each compute environment: as a Python library for Glue, as a Lambda layer for Lambda functions, as a file on the EC2 instance, and via `requirements.txt` for MWAA.
5. WHEN demonstrating Log_Shipper usage, THE Blog SHALL show a complete call-site example for each component type (Glue job script, Lambda handler, EC2 script, MWAA DAG callback).
6. THE Blog SHALL explain how the Log_Shipper reads the OpenSearch endpoint from AWS Secrets Manager rather than from environment variables or hardcoded values.

---

### Requirement 5: OpenSearch Index Configuration

**User Story:** As a Reader, I want to know how to configure the OpenSearch index correctly, so that my Log_Events are stored efficiently and queries perform well.

#### Acceptance Criteria

1. THE Blog SHALL provide the complete OpenSearch index mapping JSON, including field types for all Log_Event fields (`keyword` for identifiers, `text` for messages, `date` for timestamps, `long` for durations).
2. THE Blog SHALL explain why `keyword` type is used for fields like `run_id`, `task_name`, `component_type`, and `log_level` (exact-match filtering and aggregations).
3. THE Blog SHALL provide the complete ISM_Policy JSON that configures 30-day retention by transitioning indices to a delete state after 30 days.
4. THE Blog SHALL show the API call or console steps to apply the index mapping and ISM_Policy to the OpenSearch domain.
5. THE Blog SHALL explain the monthly index rollover naming convention (`etl-logs-YYYY-MM`) and how it simplifies retention management.
6. WHEN configuring the OpenSearch domain, THE Blog SHALL describe the IAM access policy that grants the Log_Shipper's IAM roles the `es:ESHttpPost` permission on the index.

---

### Requirement 6: MWAA DAG Integration

**User Story:** As a Reader, I want to see how to instrument the MWAA DAG itself to emit Log_Events, so that I can monitor task lifecycle events alongside compute-component logs.

#### Acceptance Criteria

1. THE Blog SHALL show how to attach an `on_success_callback` and `on_failure_callback` to the DAG that invoke the Log_Shipper to write task lifecycle Log_Events to OpenSearch.
2. THE Blog SHALL provide a complete code example of the `notify_failure` callback that publishes to SNS and writes a Log_Event to OpenSearch containing `run_id`, `task_name`, `failure_reason`, and `timestamp`.
3. THE Blog SHALL show how to write a DAG-level summary Log_Event on run completion containing `run_id`, `overall_status`, `total_duration`, and per-task status.
4. THE Blog SHALL explain how to pass the `run_id` (Airflow's `{{ run_id }}` template variable) to each downstream task so that all Log_Events from a single run share the same correlation key.
5. THE Blog SHALL show how to configure the MWAA execution IAM role to allow the DAG to write Log_Events to OpenSearch via `es:ESHttpPost`.

---

### Requirement 7: OpenSearch Dashboards and Queries

**User Story:** As a Reader, I want example dashboards and queries, so that I can immediately visualize pipeline health and investigate failures after deploying the monitoring solution.

#### Acceptance Criteria

1. THE Blog SHALL provide at least three example OpenSearch DSL queries: one that retrieves all Log_Events for a specific `run_id`, one that filters for `log_level: ERROR` events within a time range, and one that aggregates task durations by `component_type`.
2. THE Blog SHALL describe how to create an OpenSearch_Dashboard with at least the following panels: a time-series chart of Log_Events by `log_level`, a table of recent DAG runs with their overall status, and a bar chart of average task duration by `component_type`.
3. THE Blog SHALL explain how to use the `run_id` field to drill down from a failed DAG run to the specific Log_Events that caused the failure.
4. THE Blog SHALL show how to filter the OpenSearch_Dashboard by `run_id`, `task_name`, `component_type`, and time range.
5. WHERE a Reader wants to monitor pipeline SLAs, THE Blog SHALL show how to use OpenSearch aggregations to compute the 95th-percentile task duration for each `component_type`.

---

### Requirement 8: Alerting and Anomaly Detection

**User Story:** As a Reader, I want to know how to set up alerts on pipeline failures and anomalies, so that I am notified proactively when the ETL pipeline degrades.

#### Acceptance Criteria

1. THE Blog SHALL show how to configure an OpenSearch alerting monitor that triggers when the count of `log_level: ERROR` Log_Events exceeds a configurable threshold within a rolling time window.
2. THE Blog SHALL show how to configure an OpenSearch alert destination that sends notifications to an Amazon SNS topic or email address.
3. THE Blog SHALL explain the relationship between OpenSearch alerting and the MWAA `on_failure_callback` SNS notification, clarifying when each mechanism fires.
4. WHERE a Reader wants to detect anomalous task durations, THE Blog SHALL describe how to use OpenSearch anomaly detection on the `duration_ms` field to identify tasks that are running significantly longer than their historical baseline.

---

### Requirement 9: Security and IAM Best Practices

**User Story:** As a Reader, I want the blog to follow AWS security best practices, so that the monitoring solution I build does not introduce new security risks.

#### Acceptance Criteria

1. THE Blog SHALL explain the least-privilege IAM principle and show a concrete IAM policy example that grants only `es:ESHttpPost` on the specific OpenSearch index ARN, not on `*`.
2. THE Blog SHALL explain why secrets (OpenSearch endpoint, index name) are stored in AWS Secrets Manager and show the `boto3` code to retrieve them at runtime.
3. THE Blog SHALL explicitly warn Readers not to embed the OpenSearch endpoint or any credentials in DAG code, Lambda environment variables, or source control.
4. THE Blog SHALL describe the IAM roles required for each pipeline component (MWAA execution role, Glue service role, Lambda execution role, EC2 instance profile) and the OpenSearch permissions each role needs.
5. IF a Reader is using a VPC-deployed OpenSearch domain, THEN THE Blog SHALL explain the VPC endpoint and security group configuration required for the Log_Shipper to reach OpenSearch from Glue, Lambda, and EC2.

---

### Requirement 10: Reproducibility and Code Samples

**User Story:** As a Reader, I want all code samples and configuration snippets to be complete and self-consistent, so that I can reproduce the monitoring solution without having to fill in missing pieces.

#### Acceptance Criteria

1. THE Blog SHALL ensure that all Python code samples are compatible with Python 3.9 or later and include the required `import` statements.
2. THE Blog SHALL ensure that all JSON configuration samples (index mapping, ISM policy, IAM policy) are valid JSON documents that can be applied directly via the AWS CLI or OpenSearch REST API.
3. THE Blog SHALL provide the AWS CLI commands or boto3 calls needed to create the OpenSearch domain, apply the index mapping, and apply the ISM policy.
4. THE Blog SHALL list all Python package dependencies (e.g., `requests`, `requests-aws4auth`, `boto3`) with pinned version numbers.
5. WHEN the Blog references an Airflow provider package, THE Blog SHALL specify the minimum provider version required (e.g., `apache-airflow-providers-amazon>=8.0.0`).
6. THE Blog SHALL include a troubleshooting section that addresses at least three common issues: Log_Events not appearing in OpenSearch, SigV4 authentication errors, and OpenSearch index mapping conflicts.
