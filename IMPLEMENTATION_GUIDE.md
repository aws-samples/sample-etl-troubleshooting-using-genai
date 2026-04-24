# Implementation Guide: MWAA + OpenSearch Monitoring Solution

This guide reflects the actual deployment steps used to stand up the solution in AWS account `172542676092` (us-east-1). It includes real-world fixes discovered during deployment.

---

## Deployed Resources (Reference)

| Resource | Name / ARN |
|---|---|
| OpenSearch Domain | `etl-monitoring` — `search-etl-monitoring-mz7zpvh76up33iejfbl7ock3oq.us-east-1.es.amazonaws.com` |
| OpenSearch Index | `etl-logs-2026-04` |
| ISM Retention Policy | `etl-logs-retention` (30-day) |
| OpenSearch Alert Monitor | `ETL Pipeline Error Rate Monitor` |
| SNS Notification Channel | `ETL Failures SNS Topic` |
| SNS Topic | `arn:aws:sns:us-east-1:172542676092:etl-failures` |
| Secrets Manager Secret | `etl/opensearch/endpoint` |
| S3 Bucket | `mwaa-etl-monitoring-172542676092` |
| MWAA Environment | `etl-monitoring-mwaa` (Airflow 2.9.2, PUBLIC_ONLY) |
| IAM Role | `MWAAExecutionRole` |
| IAM Role | `OpenSearchAlertingRole` |
| Private Subnets | `subnet-0029d8c9c07af1507` (us-east-1a), `subnet-0bd713831826f72fe` (us-east-1b) |
| NAT Gateway | `nat-08c30eb68d0fc97f3` |
| OpenSearch Dashboard | `ETL Pipeline Monitoring` |

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

### Step 2.1 — Create the MWAAExecutionRole

```bash
aws iam create-role \
  --role-name MWAAExecutionRole \
  --assume-role-policy-document file://infra/mwaa-trust-policy.json \
  --description "MWAA execution role for ETL monitoring pipeline"

aws iam put-role-policy \
  --role-name MWAAExecutionRole \
  --policy-name MWAAExecutionPolicy \
  --policy-document file://infra/mwaa-execution-policy.json
```

The `infra/mwaa-execution-policy.json` grants:
- `airflow:PublishMetrics` on the MWAA environment
- `s3:GetObject*`, `s3:List*` on the MWAA S3 bucket
- `logs:*` on Airflow log groups
- `cloudwatch:PutMetricData`
- `sqs:*` on Airflow Celery queues
- `es:ESHttpPost` on `etl-logs-*` index
- `secretsmanager:GetSecretValue` on `etl/opensearch/*`
- `sns:Publish` on the `etl-failures` topic

### Step 2.2 — Create the OpenSearchAlertingRole

```bash
aws iam create-role \
  --role-name OpenSearchAlertingRole \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"es.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam put-role-policy \
  --role-name OpenSearchAlertingRole \
  --policy-name SNSPublishPolicy \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sns:Publish","Resource":"arn:aws:sns:REGION:ACCOUNT_ID:etl-failures"}]}'
```

---

## Phase 3: AWS Infrastructure

### Step 3.1 — Create the SNS topic

```bash
aws sns create-topic --name etl-failures --region us-east-1
```

Subscribe your email:

```bash
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:etl-failures \
  --protocol email \
  --notification-endpoint your@email.com
```

### Step 3.2 — Create the OpenSearch domain

> **Note:** The access policy must be passed as a single-line JSON string (no newlines) to avoid a ValidationException.

```bash
aws opensearch create-domain \
  --domain-name etl-monitoring \
  --engine-version "OpenSearch_2.11" \
  --cluster-config "InstanceType=t3.medium.search,InstanceCount=1" \
  --ebs-options "EBSEnabled=true,VolumeType=gp3,VolumeSize=20" \
  --node-to-node-encryption-options "Enabled=true" \
  --encryption-at-rest-options "Enabled=true" \
  --domain-endpoint-options "EnforceHTTPS=true" \
  --advanced-security-options "Enabled=false" \
  --access-policies '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::ACCOUNT_ID:user/YOUR-IAM-USER"},"Action":"es:*","Resource":"arn:aws:es:REGION:ACCOUNT_ID:domain/etl-monitoring/*"}]}' \
  --region us-east-1
```

Wait ~15 minutes for the domain to become active:

```bash
aws opensearch describe-domain --domain-name etl-monitoring \
  --query "DomainStatus.{Processing:Processing,Endpoint:Endpoint}"
```

> **Browser access:** To access OpenSearch Dashboards from your browser, add your IP to the access policy:
> ```bash
> MY_IP=$(curl -s https://checkip.amazonaws.com)
> aws opensearch update-domain-config --domain-name etl-monitoring \
>   --access-policies "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::ACCOUNT_ID:user/YOUR-IAM-USER\"},\"Action\":\"es:*\",\"Resource\":\"arn:aws:es:REGION:ACCOUNT_ID:domain/etl-monitoring/*\"},{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"*\"},\"Action\":\"es:*\",\"Resource\":\"arn:aws:es:REGION:ACCOUNT_ID:domain/etl-monitoring/*\",\"Condition\":{\"IpAddress\":{\"aws:SourceIp\":\"$MY_IP/32\"}}}]}"
> ```

### Step 3.3 — Store the OpenSearch endpoint in Secrets Manager

```bash
aws secretsmanager create-secret \
  --name etl/opensearch/endpoint \
  --secret-string '{"opensearch_endpoint": "https://YOUR-DOMAIN-ENDPOINT"}'
```

### Step 3.4 — Create the S3 bucket for MWAA

```bash
aws s3 mb s3://mwaa-etl-monitoring-ACCOUNT_ID --region us-east-1
aws s3api put-bucket-versioning \
  --bucket mwaa-etl-monitoring-ACCOUNT_ID \
  --versioning-configuration Status=Enabled
aws s3api put-public-access-block \
  --bucket mwaa-etl-monitoring-ACCOUNT_ID \
  --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

### Step 3.5 — Create private subnets and NAT Gateway for MWAA

> **Important:** MWAA requires private subnets. The default VPC only has public subnets, so you must create private ones.

```bash
# Create two private subnets in different AZs
SUBNET_1=$(aws ec2 create-subnet \
  --vpc-id YOUR-VPC-ID \
  --cidr-block 172.31.96.0/20 \
  --availability-zone us-east-1a \
  --query "Subnet.SubnetId" --output text)

SUBNET_2=$(aws ec2 create-subnet \
  --vpc-id YOUR-VPC-ID \
  --cidr-block 172.31.112.0/20 \
  --availability-zone us-east-1b \
  --query "Subnet.SubnetId" --output text)

# Allocate an Elastic IP and create a NAT Gateway in a public subnet
EIP=$(aws ec2 allocate-address --domain vpc --query "AllocationId" --output text)

NAT_GW=$(aws ec2 create-nat-gateway \
  --subnet-id YOUR-PUBLIC-SUBNET-ID \
  --allocation-id $EIP \
  --query "NatGateway.NatGatewayId" --output text)

# Wait for NAT Gateway to be available
aws ec2 wait nat-gateway-available --nat-gateway-ids $NAT_GW

# Create a private route table and route traffic through the NAT Gateway
RT=$(aws ec2 create-route-table --vpc-id YOUR-VPC-ID --query "RouteTable.RouteTableId" --output text)
aws ec2 associate-route-table --route-table-id $RT --subnet-id $SUBNET_1
aws ec2 associate-route-table --route-table-id $RT --subnet-id $SUBNET_2
aws ec2 create-route --route-table-id $RT --destination-cidr-block 0.0.0.0/0 --nat-gateway-id $NAT_GW
```

### Step 3.6 — Create the MWAA environment

```bash
aws mwaa create-environment \
  --name etl-monitoring-mwaa \
  --airflow-version "2.9.2" \
  --execution-role-arn arn:aws:iam::ACCOUNT_ID:role/MWAAExecutionRole \
  --source-bucket-arn arn:aws:s3:::mwaa-etl-monitoring-ACCOUNT_ID \
  --dag-s3-path dags/ \
  --requirements-s3-path requirements.txt \
  --webserver-access-mode PUBLIC_ONLY \
  --network-configuration "SubnetIds=$SUBNET_1,$SUBNET_2,SecurityGroupIds=YOUR-SECURITY-GROUP-ID" \
  --logging-configuration '{"DagProcessingLogs":{"Enabled":true,"LogLevel":"INFO"},"SchedulerLogs":{"Enabled":true,"LogLevel":"INFO"},"TaskLogs":{"Enabled":true,"LogLevel":"INFO"},"WebserverLogs":{"Enabled":true,"LogLevel":"INFO"},"WorkerLogs":{"Enabled":true,"LogLevel":"INFO"}}' \
  --environment-class mw1.small \
  --max-workers 2 \
  --min-workers 1 \
  --region us-east-1
```

> **Note:** Set `--webserver-access-mode PUBLIC_ONLY` so you can access the Airflow UI from your browser. Wait ~25 minutes for the environment to become `AVAILABLE`.

---

## Phase 4: OpenSearch Index Configuration

### Step 4.1 — Create the monthly index with the field mapping

```bash
awscurl --service es --region us-east-1 \
  -X PUT \
  -H "Content-Type: application/json" \
  -d @blog/config/index_mapping.json \
  "https://YOUR-OPENSEARCH-ENDPOINT/etl-logs-$(date +%Y-%m)"
```

### Step 4.2 — Apply the ISM 30-day retention policy

```bash
awscurl --service es --region us-east-1 \
  -X PUT \
  -H "Content-Type: application/json" \
  -d @blog/config/ism_policy.json \
  "https://YOUR-OPENSEARCH-ENDPOINT/_plugins/_ism/policies/etl-logs-retention"
```

---

## Phase 5: Deploy DAG Files to MWAA

> **Important:** When deploying to MWAA, the DAG files must use flat imports (not `blog.code.*` package paths) since all files are uploaded to the same `dags/` folder.

Use the MWAA-compatible versions in `infra/`:

```bash
aws s3 cp infra/etl_workflow_dag_mwaa.py s3://mwaa-etl-monitoring-ACCOUNT_ID/dags/etl_workflow_dag.py
aws s3 cp infra/dag_callbacks_mwaa.py s3://mwaa-etl-monitoring-ACCOUNT_ID/dags/dag_callbacks.py
aws s3 cp infra/log_shipper_mwaa.py s3://mwaa-etl-monitoring-ACCOUNT_ID/dags/log_shipper.py
aws s3 cp blog/code/log_event.py s3://mwaa-etl-monitoring-ACCOUNT_ID/dags/log_event.py
aws s3 cp infra/requirements.txt s3://mwaa-etl-monitoring-ACCOUNT_ID/requirements.txt
```

> **Note:** The `infra/` versions replace `from blog.code.X import Y` with `from X import Y` to work in the flat MWAA dags folder.

> **Note:** `SsmRunCommandOperator` is not available in all versions of `apache-airflow-providers-amazon`. The `infra/etl_workflow_dag_mwaa.py` uses `PythonOperator` + `boto3` SSM client instead.

---

## Phase 6: Set Airflow Variables

Access the Airflow UI:

```bash
# Generate a login token (expires in 60 seconds — click immediately)
TOKEN=$(aws mwaa create-web-login-token --name etl-monitoring-mwaa --region us-east-1 --query "WebToken" --output text)
echo "https://YOUR-MWAA-WEBSERVER-URL/aws_mwaa/aws-console-sso?login=true#token=$TOKEN"
```

> **Tip:** Use the AWS Console → MWAA → click your environment → **Open Airflow UI** button to avoid token expiry issues.

In the Airflow UI go to **Admin → Variables → +** and add:

| Key | Value |
|---|---|
| `opensearch_secret_name` | `etl/opensearch/endpoint` |
| `opensearch_index_name` | `etl-logs-YYYY-MM` (current month) |
| `aws_region` | `us-east-1` |
| `sns_failure_topic_arn` | `arn:aws:sns:us-east-1:ACCOUNT_ID:etl-failures` |
| `glue_job_name` | Your Glue job name |
| `lambda_function_name` | Your Lambda function name |
| `ec2_instance_id` | Your EC2 instance ID |

---

## Phase 7: Configure OpenSearch Alerting

### Step 7.1 — Create the SNS notification channel

```bash
awscurl --service es --region us-east-1 \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"config":{"name":"ETL Failures SNS Topic","description":"SNS channel for ETL pipeline failures","config_type":"sns","is_enabled":true,"sns":{"topic_arn":"arn:aws:sns:us-east-1:ACCOUNT_ID:etl-failures","role_arn":"arn:aws:iam::ACCOUNT_ID:role/OpenSearchAlertingRole"}}}' \
  "https://YOUR-OPENSEARCH-ENDPOINT/_plugins/_notifications/configs"
```

Note the `config_id` from the response.

> **Note:** OpenSearch 2.x uses the Notifications plugin (`_plugins/_notifications/configs`) instead of the legacy Destinations API. The Alerting UI will show a message saying "Destinations have become channels in Notifications" — this is expected.

### Step 7.2 — Apply the alert monitor

Update `blog/config/alert_monitor.json` replacing `"sns-destination-placeholder"` with the `config_id` from Step 7.1, then:

```bash
awscurl --service es --region us-east-1 \
  -X POST \
  -H "Content-Type: application/json" \
  -d @blog/config/alert_monitor.json \
  "https://YOUR-OPENSEARCH-ENDPOINT/_plugins/_alerting/monitors"
```

The monitor checks every 5 minutes and fires when ERROR event count > 5 in the rolling window.

---

## Phase 8: Set Up OpenSearch Dashboard

Create the index pattern, visualizations, and dashboard via the API:

```bash
OS_ENDPOINT="https://YOUR-OPENSEARCH-ENDPOINT"

# 1. Create index pattern
awscurl --service es --region us-east-1 -X POST \
  -H "Content-Type: application/json" -H "osd-xsrf: true" \
  -d '{"attributes":{"title":"etl-logs-*","timeFieldName":"timestamp"}}' \
  "$OS_ENDPOINT/_dashboards/api/saved_objects/index-pattern/etl-logs-pattern"

# 2. Create visualizations and dashboard
# (See infra/create_dashboard.sh for the full script)
```

Access the dashboard at:
```
https://YOUR-OPENSEARCH-ENDPOINT/_dashboards/app/dashboards#/view/etl-monitoring-dashboard
```

---

## Phase 9: Verify End-to-End

### Step 9.1 — Trigger a test DAG run

In the Airflow UI, find `etl_workflow`, toggle it on, and click **Trigger DAG**.

### Step 9.2 — Query OpenSearch for events

```bash
awscurl --service es --region us-east-1 \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"query":{"match_all":{}},"sort":[{"timestamp":{"order":"desc"}}],"size":10}' \
  "https://YOUR-OPENSEARCH-ENDPOINT/etl-logs-$(date +%Y-%m)/_search"
```

You should see `Log_Event` documents with `run_id`, `task_name`, `log_level`, and `timestamp`.

---

## Phase 10: Ongoing Operations

### Monthly index creation

```bash
awscurl --service es --region us-east-1 \
  -X PUT \
  -H "Content-Type: application/json" \
  -d @blog/config/index_mapping.json \
  "https://YOUR-OPENSEARCH-ENDPOINT/etl-logs-$(date +%Y-%m)"
```

Update the `opensearch_index_name` Airflow Variable to the new month.

### Rotating the OpenSearch endpoint secret

```bash
aws secretsmanager update-secret \
  --secret-id etl/opensearch/endpoint \
  --secret-string '{"opensearch_endpoint": "https://NEW-DOMAIN-ENDPOINT"}'
```

---

## Troubleshooting

| Issue | Likely Cause | Resolution |
|---|---|---|
| Log_Events not appearing in OpenSearch | IAM role missing `es:ESHttpPost`, wrong index name, or wrong endpoint | Check `LogShipper` logs for HTTP 403. Verify the IAM policy ARN matches the domain ARN exactly. |
| SigV4 authentication errors | `requests-aws4auth` not installed, wrong region, or IAM role not attached | Confirm `requests-aws4auth==1.3.1` is installed. Verify the `region` matches the OpenSearch domain region. |
| OpenSearch index mapping conflicts | Field type mismatch | Delete and recreate the index with `blog/config/index_mapping.json`. |
| MWAA DAG broken — `ModuleNotFoundError: No module named 'blog'` | DAG files use `blog.code.*` imports but MWAA uses a flat `dags/` folder | Use the MWAA-compatible files in `infra/` which use flat imports (`from log_event import ...`). |
| MWAA DAG broken — `SsmRunCommandOperator` not found | Operator not available in installed provider version | Use `PythonOperator` + `boto3` SSM client instead (already done in `infra/etl_workflow_dag_mwaa.py`). |
| MWAA DAG broken — `poll_interval` invalid argument | `GlueJobOperator` version doesn't support this parameter | Remove `poll_interval` from `GlueJobOperator` constructor. |
| MWAA webserver link gives "permission" error | Login token expired (60-second TTL) | Generate and click the token URL in the same command: `TOKEN=$(aws mwaa create-web-login-token ...) && echo "https://...#token=$TOKEN"` |
| OpenSearch Dashboards "anonymous user" error | Browser access not allowed by domain access policy | Add your IP to the OpenSearch access policy (see Phase 3.2). |
| Destinations API returns 405 | OpenSearch 2.x uses Notifications plugin, not legacy Destinations | Use `_plugins/_notifications/configs` endpoint instead. |
| MWAA environment creation fails — "subnets must be private" | Default VPC only has public subnets | Create private subnets with a NAT Gateway (see Phase 3.5). |

---

## Reference: File Locations

| File | Purpose |
|---|---|
| `blog/code/log_event.py` | `LogEvent` dataclass — shared schema |
| `blog/code/log_shipper.py` | `LogShipper` class — SigV4 HTTP client (original, uses `blog.code.*` imports) |
| `blog/code/dag_callbacks.py` | MWAA DAG callback functions (original) |
| `blog/code/etl_workflow_dag.py` | MWAA DAG definition (original) |
| `infra/log_shipper_mwaa.py` | `LogShipper` — MWAA-compatible (flat imports) |
| `infra/dag_callbacks_mwaa.py` | DAG callbacks — MWAA-compatible (flat imports) |
| `infra/etl_workflow_dag_mwaa.py` | DAG definition — MWAA-compatible (flat imports, PythonOperator for SSM) |
| `infra/requirements.txt` | Python dependencies for MWAA |
| `infra/mwaa-trust-policy.json` | Trust policy for MWAAExecutionRole |
| `infra/mwaa-execution-policy.json` | Permissions policy for MWAAExecutionRole |
| `infra/opensearch-alerting-trust-policy.json` | Trust policy for OpenSearchAlertingRole |
| `blog/config/index_mapping.json` | OpenSearch index field mapping |
| `blog/config/ism_policy.json` | ISM 30-day retention policy |
| `blog/config/iam_policy_opensearch.json` | Least-privilege IAM policy for compute roles |
| `blog/config/alert_monitor.json` | OpenSearch alerting monitor config |
| `blog/config/dsl_query_by_run_id.json` | Query: all events for a run |
| `blog/config/dsl_query_errors.json` | Query: ERROR events in time range |
| `blog/config/dsl_query_duration_agg.json` | Query: avg duration by component |
| `blog/config/dsl_query_p95_duration.json` | Query: p95 duration by component |

---

## Phase 11: Natural Language Log Querying with the OpenSearch MCP Server

### Step 11.1 — Install the OpenSearch MCP Server

```bash
pip install opensearch-mcp-server-py
```

Or use `uvx` (no install required):

```bash
uvx opensearch-mcp-server-py@latest
```

### Step 11.2 — Configure your MCP client

Add the following to your MCP client config. For Amazon Kiro, edit `~/.kiro/settings/mcp.json`:

```json
{
  "mcpServers": {
    "opensearch-mcp": {
      "command": "uvx",
      "args": ["opensearch-mcp-server-py@latest"],
      "env": {
        "OPENSEARCH_URL": "https://search-etl-monitoring-mz7zpvh76up33iejfbl7ock3oq.us-east-1.es.amazonaws.com",
        "AWS_REGION": "us-east-1",
        "OPENSEARCH_AUTH_TYPE": "awssigv4",
        "OPENSEARCH_SERVICE": "es",
        "FASTMCP_LOG_LEVEL": "ERROR"
      },
      "disabled": false,
      "autoApprove": [
        "SearchIndexTool",
        "ListIndexTool",
        "IndexMappingTool",
        "ClusterHealthTool",
        "CountTool"
      ]
    }
  }
}
```

### Step 11.3 — Reconnect the MCP server

In Kiro: open the MCP panel (Command Palette → "MCP"), find `opensearch-mcp`, and click **Reconnect**.

### Step 11.4 — Verify the connection

Ask the AI: *"How many events are in the etl-logs-2026-04 index?"*

Expected response: a count of documents confirming the server is connected.

### Step 11.5 — Example queries

Once connected, you can ask natural language questions directly:

| Question | What it does |
|---|---|
| "Show me all ERROR events from the last hour" | Queries `log_level: ERROR` with a timestamp range filter |
| "What tasks ran successfully today?" | Queries `log_level: INFO` grouped by `task_name` |
| "Show me all events for run_id X" | Queries by exact `run_id` match, sorted by timestamp |
| "What is the average duration for each component type?" | Runs a `terms` + `avg` aggregation on `duration_ms` |
| "How many total log events are in the index?" | Runs a `count` query |
| "Show me the most recent pipeline failures" | Queries `log_level: ERROR` sorted by timestamp descending |

### Notes

- Authentication uses your existing AWS credentials via SigV4 — no username or password needed
- The server requires `es:ESHttpGet` and `es:ESHttpPost` permissions on the OpenSearch domain
- If your domain uses IP-based access control, ensure your machine's IP is in the access policy
- The `autoApprove` list controls which tools run without confirmation — add or remove tools as needed
