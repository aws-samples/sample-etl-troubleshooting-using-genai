# Implementation Guide: MWAA + OpenSearch Monitoring Solution

This guide walks through the concrete steps to deploy the monitoring solution described in the blog post. Follow the steps in order — each phase builds on the previous one.

---

## Phase 1: Prerequisites

### Step 1.1 — Verify AWS services are active

Confirm the following services are available in your target AWS region:

- Amazon MWAA (Airflow 2.6+)
- AWS Glue
- AWS Lambda
- Amazon EC2
- Amazon OpenSearch Service (OpenSearch 2.x or later)
- AWS Secrets Manager
- Amazon SNS

### Step 1.2 — Install local tooling

```bash
pip install awscurl          # SigV4-signed CLI requests to OpenSearch
pip install boto3>=1.34.0    # AWS SDK
```

### Step 1.3 — Clone this repository

```bash
git clone git@ssh.gitlab.aws.dev:bjurstro/MWAA_Observability_Blog.git
cd MWAA_Observability_Blog
```

---

## Phase 2: IAM Setup

### Step 2.1 — Create or update the four compute IAM roles

Each of the following roles needs the OpenSearch write permission below. Attach it as an inline or managed policy.

| Role | Used by |
|---|---|
| `MWAAExecutionRole` | MWAA environment |
| `GlueServiceRole` | AWS Glue job |
| `LambdaExecutionRole` | Lambda function |
| `EC2InstanceProfile` | EC2 instance |

Apply `blog/config/iam_policy_opensearch.json` to each role, replacing the placeholder values:

```bash
# Replace REGION, ACCOUNT_ID, DOMAIN_NAME with your values
aws iam put-role-policy \
  --role-name MWAAExecutionRole \
  --policy-name OpenSearchETLLogsWrite \
  --policy-document file://blog/config/iam_policy_opensearch.json
```

Repeat for `GlueServiceRole`, `LambdaExecutionRole`, and `EC2InstanceProfile`.

### Step 2.2 — Add Secrets Manager permission to MWAAExecutionRole

```bash
aws iam put-role-policy \
  --role-name MWAAExecutionRole \
  --policy-name SecretsManagerRead \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": "arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:etl/opensearch/*"
    }]
  }'
```

### Step 2.3 — Add SNS publish permission to MWAAExecutionRole

```bash
aws iam put-role-policy \
  --role-name MWAAExecutionRole \
  --policy-name SNSFailureNotify \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["sns:Publish"],
      "Resource": "arn:aws:sns:REGION:ACCOUNT_ID:etl-failures"
    }]
  }'
```

---

## Phase 3: AWS Infrastructure

### Step 3.1 — Create the SNS topic for failure notifications

```bash
aws sns create-topic --name etl-failures
# Note the TopicArn in the output — you'll need it in later steps
```

Subscribe your email or PagerDuty endpoint:

```bash
aws sns subscribe \
  --topic-arn arn:aws:sns:REGION:ACCOUNT_ID:etl-failures \
  --protocol email \
  --notification-endpoint your@email.com
```

### Step 3.2 — Create the Amazon OpenSearch Service domain

If you don't have an existing domain, create one. Minimum recommended configuration:

```bash
aws opensearch create-domain \
  --domain-name etl-monitoring \
  --engine-version OpenSearch_2.11 \
  --cluster-config InstanceType=t3.medium.search,InstanceCount=1 \
  --ebs-options EBSEnabled=true,VolumeType=gp3,VolumeSize=20 \
  --access-policies '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "arn:aws:iam::ACCOUNT_ID:role/MWAAExecutionRole",
          "arn:aws:iam::ACCOUNT_ID:role/GlueServiceRole",
          "arn:aws:iam::ACCOUNT_ID:role/LambdaExecutionRole",
          "arn:aws:iam::ACCOUNT_ID:role/EC2InstanceProfile"
        ]
      },
      "Action": "es:ESHttpPost",
      "Resource": "arn:aws:es:REGION:ACCOUNT_ID:domain/etl-monitoring/etl-logs-*"
    }]
  }'
```

Wait for the domain status to become `Active` (typically 10–15 minutes):

```bash
aws opensearch describe-domain --domain-name etl-monitoring \
  --query 'DomainStatus.Processing'
```

Note the domain endpoint:

```bash
aws opensearch describe-domain --domain-name etl-monitoring \
  --query 'DomainStatus.Endpoints.vpc // DomainStatus.Endpoint'
```

### Step 3.3 — Store the OpenSearch endpoint in Secrets Manager

```bash
aws secretsmanager create-secret \
  --name etl/opensearch/endpoint \
  --secret-string '{"opensearch_endpoint": "https://YOUR-DOMAIN-ENDPOINT"}'
```

---

## Phase 4: OpenSearch Index Configuration

### Step 4.1 — Create the monthly index with the field mapping

Replace `<OPENSEARCH_ENDPOINT>` and `<AWS_REGION>` with your values. Run this at the start of each month (or automate it with a Lambda/MWAA task).

```bash
awscurl --service es --region <AWS_REGION> \
  -X PUT \
  -H "Content-Type: application/json" \
  -d @blog/config/index_mapping.json \
  "https://<OPENSEARCH_ENDPOINT>/etl-logs-$(date +%Y-%m)"
```

Expected response:

```json
{"acknowledged": true, "shards_acknowledged": true, "index": "etl-logs-2024-03"}
```

### Step 4.2 — Apply the ISM 30-day retention policy

```bash
awscurl --service es --region <AWS_REGION> \
  -X PUT \
  -H "Content-Type: application/json" \
  -d @blog/config/ism_policy.json \
  "https://<OPENSEARCH_ENDPOINT>/_plugins/_ism/policies/etl-logs-retention"
```

### Step 4.3 — Verify the ISM policy is attached to the index

```bash
awscurl --service es --region <AWS_REGION> \
  -X GET \
  "https://<OPENSEARCH_ENDPOINT>/_plugins/_ism/explain/etl-logs-$(date +%Y-%m)"
```

---

## Phase 5: Deploy the LogShipper Library

### Step 5.1 — Deploy to AWS Glue

Add the following to your Glue job's `--additional-python-modules` parameter:

```
requests==2.31.0,requests-aws4auth==1.3.1,boto3>=1.34.0
```

Upload `blog/code/log_event.py` and `blog/code/log_shipper.py` to S3 and add the S3 path to `--extra-py-files`:

```bash
aws s3 cp blog/code/log_event.py s3://YOUR-BUCKET/glue-libs/log_event.py
aws s3 cp blog/code/log_shipper.py s3://YOUR-BUCKET/glue-libs/log_shipper.py
```

In your Glue job configuration:

```
--extra-py-files s3://YOUR-BUCKET/glue-libs/log_event.py,s3://YOUR-BUCKET/glue-libs/log_shipper.py
```

### Step 5.2 — Deploy to AWS Lambda as a layer

```bash
mkdir -p python/blog/code
cp blog/code/log_event.py blog/code/log_shipper.py python/blog/code/
pip install requests==2.31.0 requests-aws4auth==1.3.1 -t python/
zip -r log_shipper_layer.zip python/

aws lambda publish-layer-version \
  --layer-name log-shipper \
  --zip-file fileb://log_shipper_layer.zip \
  --compatible-runtimes python3.9 python3.10 python3.11
```

Attach the layer to your Lambda function:

```bash
aws lambda update-function-configuration \
  --function-name YOUR-FUNCTION-NAME \
  --layers arn:aws:lambda:REGION:ACCOUNT_ID:layer:log-shipper:1
```

Add the required environment variables to the Lambda function:

```bash
aws lambda update-function-configuration \
  --function-name YOUR-FUNCTION-NAME \
  --environment Variables='{
    "SECRET_NAME": "etl/opensearch/endpoint",
    "INDEX_NAME": "etl-logs-2024-03"
  }'
```

### Step 5.3 — Deploy to EC2

SSH into the EC2 instance and install the dependencies:

```bash
pip install requests==2.31.0 requests-aws4auth==1.3.1 boto3>=1.34.0
```

Copy the library files to the instance:

```bash
scp blog/code/log_event.py blog/code/log_shipper.py ec2-user@YOUR-INSTANCE:/opt/etl/
```

### Step 5.4 — Deploy to MWAA

Add the following lines to your MWAA environment's `requirements.txt`:

```
requests==2.31.0
requests-aws4auth==1.3.1
boto3>=1.34.0
apache-airflow-providers-amazon>=8.0.0
```

Upload the updated `requirements.txt` to S3 and update the MWAA environment:

```bash
aws s3 cp requirements.txt s3://YOUR-MWAA-BUCKET/requirements.txt

aws mwaa update-environment \
  --name YOUR-MWAA-ENV \
  --requirements-s3-path requirements.txt
```

---

## Phase 6: Instrument the Compute Components

### Step 6.1 — Update the Glue job script

Replace your existing Glue job script with the pattern from `blog/code/glue_job_example.py`. Key changes:

1. Add `getResolvedOptions` args: `run_id`, `task_name`, `secret_name`, `index_name`, `region`
2. Retrieve the OpenSearch endpoint via `LogShipper._get_endpoint_from_secrets_manager()`
3. Wrap your ETL logic in `try/except/finally` and ship a `LogEvent` in the `finally` block

### Step 6.2 — Update the Lambda handler

Replace your existing Lambda handler with the pattern from `blog/code/lambda_handler_example.py`. Key changes:

1. Instantiate `LogShipper` at module scope (outside the handler) for warm reuse
2. Read `SECRET_NAME` and `INDEX_NAME` from environment variables
3. Wrap your handler logic in `try/except/finally` and ship a `LogEvent` in the `finally` block

### Step 6.3 — Update the EC2 script

Replace your existing EC2 script with the pattern from `blog/code/ec2_script_example.py`. Key changes:

1. Add CLI args: `--run-id`, `--task-name`, `--secret-name`, `--index-name`, `--region`
2. Retrieve the instance ID via IMDSv2
3. Wrap your processing logic in `try/except/finally` and ship a `LogEvent` in the `finally` block

### Step 6.4 — Deploy the MWAA DAG

Upload the DAG files to your MWAA S3 bucket:

```bash
aws s3 cp blog/code/dag_callbacks.py s3://YOUR-MWAA-BUCKET/dags/dag_callbacks.py
aws s3 cp blog/code/etl_workflow_dag.py s3://YOUR-MWAA-BUCKET/dags/etl_workflow_dag.py
```

Set the required Airflow Variables in the MWAA UI or via CLI:

```bash
# In the Airflow UI: Admin → Variables → Add
opensearch_secret_name  = etl/opensearch/endpoint
opensearch_index_name   = etl-logs-2024-03
aws_region              = us-east-1
sns_failure_topic_arn   = arn:aws:sns:REGION:ACCOUNT_ID:etl-failures
glue_job_name           = YOUR-GLUE-JOB-NAME
lambda_function_name    = YOUR-LAMBDA-FUNCTION-NAME
ec2_instance_id         = YOUR-EC2-INSTANCE-ID
```

---

## Phase 7: Verify End-to-End

### Step 7.1 — Trigger a test DAG run

In the Airflow UI, trigger the `etl_workflow` DAG manually. Wait for it to complete.

### Step 7.2 — Query OpenSearch for the run's events

Replace `<RUN_ID>` with the DAG run ID from the Airflow UI (e.g., `manual__2024-03-15T10:00:00+00:00`):

```bash
awscurl --service es --region <AWS_REGION> \
  -X GET \
  -H "Content-Type: application/json" \
  -d '{"query": {"term": {"run_id": "<RUN_ID>"}}, "sort": [{"timestamp": {"order": "asc"}}]}' \
  "https://<OPENSEARCH_ENDPOINT>/etl-logs-$(date +%Y-%m)/_search"
```

You should see `Log_Event` documents from all four component types (glue, lambda, ec2, mwaa).

### Step 7.3 — Verify error events are captured

Intentionally fail a task (e.g., pass an invalid input to the Glue job) and confirm an `ERROR`-level `Log_Event` appears:

```bash
awscurl --service es --region <AWS_REGION> \
  -X GET \
  -H "Content-Type: application/json" \
  -d @blog/config/dsl_query_errors.json \
  "https://<OPENSEARCH_ENDPOINT>/etl-logs-$(date +%Y-%m)/_search"
```

---

## Phase 8: Build OpenSearch Dashboards

### Step 8.1 — Create the index pattern

1. Open OpenSearch Dashboards → **Stack Management → Index Patterns → Create index pattern**
2. Pattern: `etl-logs-*`
3. Time field: `timestamp`
4. Click **Create index pattern**

### Step 8.2 — Create the pipeline health dashboard

Create a new dashboard with the following panels:

**Panel 1 — Event count by log level (time series)**
- Visualization type: Line or Area
- X-axis: `timestamp` (date histogram, auto interval)
- Split series: `log_level` (terms aggregation)

**Panel 2 — Recent DAG runs (data table)**
- Visualization type: Data Table
- Split rows: `run_id` (terms, ordered by `timestamp` desc, size 20)
- Metric columns: Count, filtered count for `log_level: ERROR`

**Panel 3 — Average task duration by component (bar chart)**
- Visualization type: Vertical Bar
- X-axis: `component_type` (terms)
- Y-axis: `avg(duration_ms)`

### Step 8.3 — Add dashboard filters

Add filter controls for: `run_id`, `task_name`, `component_type`, and the global time range picker.

---

## Phase 9: Configure Alerting

### Step 9.1 — Create the SNS destination in OpenSearch Dashboards

1. Navigate to **Alerting → Destinations → Add destination**
2. Name: `ETL Failures SNS`
3. Type: `Amazon SNS`
4. SNS Topic ARN: `arn:aws:sns:REGION:ACCOUNT_ID:etl-failures`
5. IAM Role ARN: an IAM role with `sns:Publish` that OpenSearch Alerting can assume
6. Save and note the destination ID

### Step 9.2 — Update the alert monitor config

Edit `blog/config/alert_monitor.json` and replace `"sns-destination-placeholder"` with the destination ID from Step 9.1.

### Step 9.3 — Apply the alert monitor

```bash
awscurl --service es --region <AWS_REGION> \
  -X POST \
  -H "Content-Type: application/json" \
  -d @blog/config/alert_monitor.json \
  "https://<OPENSEARCH_ENDPOINT>/_plugins/_alerting/monitors"
```

### Step 9.4 — (Optional) Enable anomaly detection on `duration_ms`

1. Navigate to **Anomaly Detection → Create detector**
2. Index: `etl-logs-*`, timestamp field: `timestamp`
3. Feature: `average(duration_ms)`
4. Category field: `component_type`
5. Detection interval: 10 minutes, window delay: 1 minute
6. Enable real-time detection and link to an alert monitor

---

## Phase 10: Ongoing Operations

### Monthly index creation

At the start of each month, create the new index:

```bash
awscurl --service es --region <AWS_REGION> \
  -X PUT \
  -H "Content-Type: application/json" \
  -d @blog/config/index_mapping.json \
  "https://<OPENSEARCH_ENDPOINT>/etl-logs-$(date +%Y-%m)"
```

Consider automating this with a scheduled MWAA DAG or EventBridge + Lambda.

### Updating the Airflow Variable for the current index

Each month, update the `opensearch_index_name` Airflow Variable to the new index name (e.g., `etl-logs-2024-04`).

### Rotating the OpenSearch endpoint secret

If you migrate to a new OpenSearch domain, update the Secrets Manager secret value. All compute components will pick up the new endpoint on their next invocation — no code changes or redeployments needed:

```bash
aws secretsmanager update-secret \
  --secret-id etl/opensearch/endpoint \
  --secret-string '{"opensearch_endpoint": "https://NEW-DOMAIN-ENDPOINT"}'
```

---

## Troubleshooting

| Issue | Likely Cause | Resolution |
|---|---|---|
| Log_Events not appearing in OpenSearch | IAM role missing `es:ESHttpPost`, wrong index name, or wrong endpoint | Check `LogShipper` logs for HTTP 403. Verify the IAM policy ARN matches the domain ARN exactly. Confirm the index name in the Airflow Variable matches the index you created. |
| SigV4 authentication errors (`AuthorizationException`) | `requests-aws4auth` not installed, wrong region, or IAM role not attached | Confirm `requests-aws4auth==1.3.1` is installed. Verify the `region` matches the OpenSearch domain region. Check the IAM role is attached to the compute resource. |
| OpenSearch index mapping conflicts | Field type mismatch between the document and the existing mapping | Delete and recreate the index with `blog/config/index_mapping.json`. The `dynamic: strict` setting will reject unknown fields with a 400 error — check the `LogShipper` error log for the rejected field name. |
| MWAA DAG not picking up new `requirements.txt` | MWAA environment update still in progress | Check the MWAA environment status in the console. Updates can take 20–30 minutes. |
| SNS notifications not firing | OpenSearch Alerting destination misconfigured or IAM role missing `sns:Publish` | Verify the destination ID in `alert_monitor.json` matches the saved destination. Check the OpenSearch Alerting role has `sns:Publish` on the topic ARN. |

---

## Reference: File Locations

| File | Purpose |
|---|---|
| `blog/code/log_event.py` | `LogEvent` dataclass — shared schema |
| `blog/code/log_shipper.py` | `LogShipper` class — SigV4 HTTP client |
| `blog/code/glue_job_example.py` | Glue job call-site example |
| `blog/code/lambda_handler_example.py` | Lambda handler call-site example |
| `blog/code/ec2_script_example.py` | EC2 script call-site example |
| `blog/code/dag_callbacks.py` | MWAA DAG callback functions |
| `blog/code/etl_workflow_dag.py` | MWAA DAG definition |
| `blog/config/index_mapping.json` | OpenSearch index field mapping |
| `blog/config/ism_policy.json` | ISM 30-day retention policy |
| `blog/config/iam_policy_opensearch.json` | Least-privilege IAM policy |
| `blog/config/dsl_query_by_run_id.json` | Query: all events for a run |
| `blog/config/dsl_query_errors.json` | Query: ERROR events in time range |
| `blog/config/dsl_query_duration_agg.json` | Query: avg duration by component |
| `blog/config/dsl_query_p95_duration.json` | Query: p95 duration by component |
| `blog/config/alert_monitor.json` | OpenSearch alerting monitor config |
