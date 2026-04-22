# MWAA ETL Workflow — Infrastructure (CDK v2 Python)

This directory contains the AWS CDK v2 (Python) stack that provisions all infrastructure for the MWAA ETL Workflow project.

## Resources provisioned

| # | Resource | Name / ID |
|---|----------|-----------|
| 1 | VPC | `etl-vpc` (2 AZs, public + private subnets, NAT gateway) |
| 2 | S3 buckets | `etl-dags-bucket`, `etl-source-bucket`, `etl-target-bucket`, `etl-output-bucket` |
| 3 | IAM roles | `etl-mwaa-execution-role`, `etl-glue-service-role`, `etl-lambda-execution-role`, `etl-ec2-instance-role` |
| 4 | Secrets Manager | `etl/opensearch/endpoint`, `etl/opensearch/index`, `etl/db/credentials`, `etl/api/keys` |
| 5 | SNS topic | `etl-pipeline-failures` |
| 6 | OpenSearch domain | `etl-logs` (OpenSearch 2.11, t3.medium.search) |
| 7 | Glue job | `etl-extraction-job` (pythonshell, Python 3.9) |
| 8 | Lambda function | `etl-transform` (Python 3.11) |
| 9 | EC2 instance | `etl-ec2-worker` (t3.medium, Amazon Linux 2023) |
| 10 | MWAA environment | `etl-mwaa` (Airflow 2.9.2, mw1.small) |

---

## Prerequisites

1. **AWS CLI** configured with credentials that have sufficient permissions to deploy CloudFormation stacks, create IAM roles, and provision all resources above.
2. **Node.js ≥ 18** (required by the CDK CLI).
3. **CDK CLI** installed globally:
   ```bash
   npm install -g aws-cdk
   ```
4. **Python 3.11+** and a virtual environment tool (`venv` or `conda`).

---

## Setup

```bash
# From the infra/ directory
cd infra/

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# Install CDK Python dependencies
pip install -r requirements.txt
```

---

## Bootstrap (first time only)

CDK requires a bootstrap stack in each account/region before the first deployment.

```bash
cdk bootstrap aws://<ACCOUNT_ID>/<REGION>
# Example:
cdk bootstrap aws://123456789012/us-east-1
```

---

## Deploy

```bash
# Preview the changes (no AWS calls)
cdk diff

# Deploy the stack
cdk deploy

# Deploy with explicit account and region
cdk deploy --context account=123456789012 --context region=us-east-1

# Override the notification email
cdk deploy --context notification_email=your-team@example.com
```

CDK will print all CloudFormation outputs at the end of a successful deployment.

---

## Post-deploy steps

### 1. Confirm the SNS email subscription

An email is sent to the `notification_email` address during deployment.  Open the email and click **Confirm subscription** before the pipeline can send failure alerts.

### 2. Replace placeholder secret values

The four Secrets Manager secrets are created with placeholder values.  Update them with real values before running the pipeline:

```bash
aws secretsmanager put-secret-value \
  --secret-id etl/opensearch/endpoint \
  --secret-string '{"endpoint":"https://search-etl-logs-xxx.us-east-1.es.amazonaws.com","region":"us-east-1"}'

aws secretsmanager put-secret-value \
  --secret-id etl/db/credentials \
  --secret-string '{"username":"myuser","password":"mypassword"}'

aws secretsmanager put-secret-value \
  --secret-id etl/api/keys \
  --secret-string '{"api_key":"my-real-api-key"}'
```

### 3. Upload DAG files and plugins to S3

```bash
# Upload the DAG
aws s3 cp ../dags/ s3://etl-dags-bucket/dags/ --recursive

# Upload the Glue job script
aws s3 cp ../glue/etl_glue_job.py s3://etl-dags-bucket/scripts/etl_glue_job.py

# Upload plugins (if any)
aws s3 cp ../plugins/ s3://etl-dags-bucket/plugins/ --recursive

# Upload the MWAA requirements file
aws s3 cp ../requirements.txt s3://etl-dags-bucket/requirements.txt
```

### 4. Update Airflow Variables

Log in to the MWAA web UI and set the following Airflow Variables (Admin → Variables):

| Key | Example value |
|-----|---------------|
| `glue_job_name` | `etl-extraction-job` |
| `lambda_function_name` | `etl-transform` |
| `ec2_instance_id` | *(value from `Ec2InstanceId` CloudFormation output)* |
| `sns_failure_topic_arn` | *(value from `SnsTopicArn` CloudFormation output)* |
| `glue_source_path` | `s3://etl-source-bucket/` |
| `glue_target_path` | `s3://etl-target-bucket/` |
| `lambda_input_path` | `s3://etl-target-bucket/` |
| `ec2_input_path` | `s3://etl-target-bucket/` |
| `ec2_output_path` | `s3://etl-output-bucket/` |

### 5. Deploy the real Lambda function code

Replace the inline placeholder with the actual handler:

```bash
cd ../lambda/
zip -r function.zip handler.py log_shipper.py
aws lambda update-function-code \
  --function-name etl-transform \
  --zip-file fileb://function.zip
```

---

## Destroy

```bash
cdk destroy
```

> **Warning**: All S3 buckets are created with `RemovalPolicy.DESTROY` and `auto_delete_objects=True` for dev/test convenience.  This means `cdk destroy` will **permanently delete all bucket contents**.  Change these settings before using this stack in production.

---

## Production checklist

Search for `# PROD:` comments in `mwaa_etl_stack.py` for a full list of changes to make before promoting to production.  Key items:

- [ ] Change `RemovalPolicy.DESTROY` → `RemovalPolicy.RETAIN` for S3, OpenSearch, and Secrets Manager.
- [ ] Remove `auto_delete_objects=True` from all S3 buckets.
- [ ] Upgrade MWAA environment class (`mw1.small` → `mw1.medium` or larger).
- [ ] Increase `max_workers` for MWAA.
- [ ] Add a second NAT gateway (one per AZ) for HA.
- [ ] Enable OpenSearch VPC endpoint and restrict access to VPC CIDR.
- [ ] Upgrade OpenSearch instance type (`t3.medium.search` → `r6g.large.search` or larger).
- [ ] Enable OpenSearch dedicated master nodes.
- [ ] Change MWAA `webserver_access_mode` to `PRIVATE_ONLY`.
- [ ] Replace the inline Lambda placeholder with real packaged code.
- [ ] Scope the MWAA SSM policy to the specific EC2 instance ARN.
