"""
MwaaEtlStack — CDK v2 (Python) stack for the MWAA ETL Workflow project.

Provisions:
  1.  VPC (2 AZs, public + private subnets, NAT gateway)
  2.  S3 buckets (DAGs, source, target, output)
  3.  IAM roles (MWAA, Glue, Lambda, EC2 instance profile)
  4.  Secrets Manager secrets (OpenSearch endpoint/index, DB creds, API keys)
  5.  SNS topic + email subscription
  6.  OpenSearch domain
  7.  AWS Glue job
  8.  Lambda function
  9.  EC2 instance (Amazon Linux 2023, SSM-managed)
  10. MWAA environment
  11. CloudFormation outputs

PRODUCTION NOTES (search for "# PROD:" comments throughout):
  - Change RemovalPolicy.DESTROY → RemovalPolicy.RETAIN for stateful resources.
  - Enable VPC endpoint for OpenSearch and use larger instance types.
  - Increase MWAA environment class and worker count.
  - Replace placeholder secret values before deploying.
"""

from __future__ import annotations

import json

import aws_cdk as cdk
from aws_cdk import (
    CfnOutput,
    RemovalPolicy,
    Stack,
    aws_ec2 as ec2,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_mwaa as mwaa,
    aws_opensearchservice as opensearch,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
)
from constructs import Construct


class MwaaEtlStack(Stack):
    """
    Self-contained CDK stack for the MWAA ETL Workflow.

    All resource names use the ``etl-`` prefix for easy identification in the
    AWS console.  No account IDs or region strings are hardcoded — they are
    resolved at synthesis time via ``Stack.of(self).account`` and
    ``Stack.of(self).region``.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Read notification email from CDK context; fall back to a safe default.
        notification_email: str = self.node.try_get_context("notification_email") or "ops@example.com"

        # ------------------------------------------------------------------
        # 1. VPC
        # ------------------------------------------------------------------
        vpc = ec2.Vpc(
            self,
            "EtlVpc",
            vpc_name="etl-vpc",
            max_azs=2,
            nat_gateways=1,  # PROD: increase to 2 for HA (one per AZ)
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        # ------------------------------------------------------------------
        # 2. S3 Buckets
        # All buckets: versioning enabled, all public access blocked.
        # PROD: change RemovalPolicy.DESTROY -> RemovalPolicy.RETAIN and
        #       set auto_delete_objects=False.
        # ------------------------------------------------------------------

        def _make_bucket(self_: MwaaEtlStack, bucket_id: str, bucket_name: str) -> s3.Bucket:
            """Helper: create a versioned, private S3 bucket."""
            return s3.Bucket(
                self_,
                bucket_id,
                bucket_name=bucket_name,
                versioned=True,
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                encryption=s3.BucketEncryption.S3_MANAGED,
                enforce_ssl=True,
                removal_policy=RemovalPolicy.DESTROY,  # PROD: change to RETAIN
                auto_delete_objects=True,              # PROD: remove this line
            )

        dags_bucket   = _make_bucket(self, "DagsBucket",   "etl-dags-bucket")
        source_bucket = _make_bucket(self, "SourceBucket", "etl-source-bucket")
        target_bucket = _make_bucket(self, "TargetBucket", "etl-target-bucket")
        output_bucket = _make_bucket(self, "OutputBucket", "etl-output-bucket")

        # ------------------------------------------------------------------
        # 4. Secrets Manager Secrets
        # Placeholder values — replace with real values before deploying.
        # PROD: change RemovalPolicy.DESTROY -> RemovalPolicy.RETAIN.
        # ------------------------------------------------------------------

        def _make_secret(
            self_: MwaaEtlStack,
            secret_id: str,
            secret_name: str,
            secret_value: dict,
        ) -> secretsmanager.Secret:
            return secretsmanager.Secret(
                self_,
                secret_id,
                secret_name=secret_name,
                description=f"ETL pipeline secret: {secret_name}",
                secret_string_value=cdk.SecretValue.unsafe_plain_text(
                    json.dumps(secret_value)
                ),
                removal_policy=RemovalPolicy.DESTROY,  # PROD: change to RETAIN
            )

        secret_opensearch_endpoint = _make_secret(
            self,
            "SecretOpenSearchEndpoint",
            "etl/opensearch/endpoint",
            {"endpoint": "REPLACE_ME", "region": "us-east-1"},
        )
        secret_opensearch_index = _make_secret(
            self,
            "SecretOpenSearchIndex",
            "etl/opensearch/index",
            {"index": "etl-logs"},
        )
        secret_db_credentials = _make_secret(
            self,
            "SecretDbCredentials",
            "etl/db/credentials",
            {"username": "REPLACE_ME", "password": "REPLACE_ME"},
        )
        secret_api_keys = _make_secret(
            self,
            "SecretApiKeys",
            "etl/api/keys",
            {"api_key": "REPLACE_ME"},
        )

        # Collect all secret ARNs for reuse in IAM policies below.
        all_secret_arns = [
            secret_opensearch_endpoint.secret_arn,
            secret_opensearch_index.secret_arn,
            secret_db_credentials.secret_arn,
            secret_api_keys.secret_arn,
        ]

        # ------------------------------------------------------------------
        # 5. SNS Topic + email subscription
        # ------------------------------------------------------------------
        failure_topic = sns.Topic(
            self,
            "EtlFailureTopic",
            topic_name="etl-pipeline-failures",
            display_name="ETL Pipeline Failures",
        )
        failure_topic.add_subscription(
            subscriptions.EmailSubscription(notification_email)
        )

        # ------------------------------------------------------------------
        # 6. OpenSearch Domain
        # Dev/test sizing: 1 x t3.medium.search, 20 GB gp3, no dedicated master.
        # PROD: use VPC endpoint, r6g.large.search or larger, dedicated master,
        #       Multi-AZ, and increase EBS volume size.
        # PROD: change RemovalPolicy.DESTROY -> RemovalPolicy.RETAIN.
        # ------------------------------------------------------------------
        opensearch_domain = opensearch.Domain(
            self,
            "EtlOpenSearchDomain",
            domain_name="etl-logs",
            version=opensearch.EngineVersion.open_search("2.11"),
            capacity=opensearch.CapacityConfig(
                data_nodes=1,
                data_node_instance_type="t3.medium.search",
                # PROD: enable dedicated master nodes:
                # master_nodes=3,
                # master_node_instance_type="t3.medium.search",
            ),
            ebs=opensearch.EbsOptions(
                enabled=True,
                volume_size=20,
                volume_type=ec2.EbsDeviceVolumeType.GP3,
            ),
            encryption_at_rest=opensearch.EncryptionAtRestOptions(enabled=True),
            node_to_node_encryption=True,
            enforce_https=True,
            # Fine-grained access control disabled; IAM resource policy used instead.
            # PROD: enable fine-grained access control for row/field-level security.
            fine_grained_access_control=opensearch.AdvancedSecurityOptions(
                master_user_arn=None,  # IAM-only mode
            ),
            removal_policy=RemovalPolicy.DESTROY,  # PROD: change to RETAIN
        )

        # ------------------------------------------------------------------
        # 3. IAM Roles (least-privilege, per design doc Section 3.3)
        # ------------------------------------------------------------------

        # ---- 3a. GlueServiceRole ----------------------------------------
        glue_role = iam.Role(
            self,
            "GlueServiceRole",
            role_name="etl-glue-service-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
            ],
            description="Glue service role for the ETL extraction job",
        )
        # Read source data
        glue_role.add_to_policy(
            iam.PolicyStatement(
                sid="GlueReadSource",
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    source_bucket.bucket_arn,
                    source_bucket.arn_for_objects("*"),
                ],
            )
        )
        # Write transformed data
        glue_role.add_to_policy(
            iam.PolicyStatement(
                sid="GlueWriteTarget",
                actions=["s3:PutObject", "s3:ListBucket"],
                resources=[
                    target_bucket.bucket_arn,
                    target_bucket.arn_for_objects("*"),
                ],
            )
        )
        # Write logs to OpenSearch
        glue_role.add_to_policy(
            iam.PolicyStatement(
                sid="GlueOpenSearchWrite",
                actions=["es:ESHttpPost", "es:ESHttpPut"],
                resources=[
                    f"{opensearch_domain.domain_arn}/*",
                ],
            )
        )

        # ---- 3b. LambdaExecutionRole ------------------------------------
        lambda_role = iam.Role(
            self,
            "LambdaExecutionRole",
            role_name="etl-lambda-execution-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
            description="Lambda execution role for the ETL transform function",
        )
        # Read from target bucket (input to Lambda)
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="LambdaReadTarget",
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    target_bucket.bucket_arn,
                    target_bucket.arn_for_objects("*"),
                ],
            )
        )
        # Write to output bucket
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="LambdaWriteOutput",
                actions=["s3:PutObject", "s3:ListBucket"],
                resources=[
                    output_bucket.bucket_arn,
                    output_bucket.arn_for_objects("*"),
                ],
            )
        )
        # Write logs to OpenSearch
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="LambdaOpenSearchWrite",
                actions=["es:ESHttpPost", "es:ESHttpPut"],
                resources=[f"{opensearch_domain.domain_arn}/*"],
            )
        )
        # Read secrets
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="LambdaReadSecrets",
                actions=["secretsmanager:GetSecretValue"],
                resources=all_secret_arns,
            )
        )

        # ---- 3c. EC2InstanceRole + InstanceProfile ----------------------
        ec2_role = iam.Role(
            self,
            "Ec2InstanceRole",
            role_name="etl-ec2-instance-role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
            ],
            description="EC2 instance role for the ETL worker instance",
        )
        # Read from target bucket (input to EC2 script)
        ec2_role.add_to_policy(
            iam.PolicyStatement(
                sid="Ec2ReadTarget",
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    target_bucket.bucket_arn,
                    target_bucket.arn_for_objects("*"),
                ],
            )
        )
        # Write to output bucket
        ec2_role.add_to_policy(
            iam.PolicyStatement(
                sid="Ec2WriteOutput",
                actions=["s3:PutObject", "s3:ListBucket"],
                resources=[
                    output_bucket.bucket_arn,
                    output_bucket.arn_for_objects("*"),
                ],
            )
        )
        # Write logs to OpenSearch
        ec2_role.add_to_policy(
            iam.PolicyStatement(
                sid="Ec2OpenSearchWrite",
                actions=["es:ESHttpPost", "es:ESHttpPut"],
                resources=[f"{opensearch_domain.domain_arn}/*"],
            )
        )
        # Read secrets
        ec2_role.add_to_policy(
            iam.PolicyStatement(
                sid="Ec2ReadSecrets",
                actions=["secretsmanager:GetSecretValue"],
                resources=all_secret_arns,
            )
        )

        ec2_instance_profile = iam.CfnInstanceProfile(
            self,
            "Ec2InstanceProfile",
            instance_profile_name="etl-ec2-instance-profile",
            roles=[ec2_role.role_name],
        )

        # ---- 3d. MWAAExecutionRole --------------------------------------
        # The Glue job, Lambda function, and EC2 instance are not yet created
        # at this point in the code, so we use placeholder ARN patterns built
        # from Stack.of(self).account / .region.  CDK resolves these tokens
        # at synthesis time.
        account = Stack.of(self).account
        region  = Stack.of(self).region

        mwaa_role = iam.Role(
            self,
            "MwaaExecutionRole",
            role_name="etl-mwaa-execution-role",
            assumed_by=iam.ServicePrincipal("airflow.amazonaws.com"),
            description="MWAA execution role for the ETL workflow environment",
        )
        # Allow MWAA service to assume this role on behalf of the environment.
        mwaa_role.assume_role_policy.add_statements(
            iam.PolicyStatement(
                principals=[iam.ServicePrincipal("airflow-env.amazonaws.com")],
                actions=["sts:AssumeRole"],
            )
        )

        # Glue permissions (scoped to the job created below)
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                sid="MwaaGlue",
                actions=["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"],
                resources=[
                    f"arn:aws:glue:{region}:{account}:job/etl-extraction-job",
                ],
            )
        )
        # Lambda invoke (scoped to the function created below)
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                sid="MwaaLambdaInvoke",
                actions=["lambda:InvokeFunction"],
                resources=[
                    f"arn:aws:lambda:{region}:{account}:function:etl-transform",
                ],
            )
        )
        # SSM Run Command on the EC2 instance (instance ARN resolved after creation)
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                sid="MwaaSsm",
                actions=["ssm:SendCommand", "ssm:GetCommandInvocation"],
                resources=[
                    # Allow SendCommand on any EC2 instance tagged for this project.
                    # PROD: scope to the specific instance ARN after first deploy.
                    f"arn:aws:ec2:{region}:{account}:instance/*",
                    # SSM document used by the DAG
                    f"arn:aws:ssm:{region}::document/AWS-RunShellScript",
                ],
            )
        )
        # SNS publish
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                sid="MwaaSnsPublish",
                actions=["sns:Publish"],
                resources=[failure_topic.topic_arn],
            )
        )
        # CloudWatch custom metrics (no resource restriction needed for PutMetricData)
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                sid="MwaaCloudWatch",
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )
        # Secrets Manager
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                sid="MwaaReadSecrets",
                actions=["secretsmanager:GetSecretValue"],
                resources=all_secret_arns,
            )
        )
        # DAGs bucket access
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                sid="MwaaDagsBucket",
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=[
                    dags_bucket.bucket_arn,
                    dags_bucket.arn_for_objects("*"),
                ],
            )
        )
        # Standard MWAA execution role permissions (CloudWatch Logs, SQS, KMS)
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                sid="MwaaLogs",
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:GetLogEvents",
                    "logs:GetLogRecord",
                    "logs:GetLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutRetentionPolicy",
                    "logs:DescribeLogGroups",
                ],
                resources=["*"],
            )
        )
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                sid="MwaaAirflowPublish",
                actions=[
                    "airflow:PublishMetrics",
                ],
                resources=[
                    f"arn:aws:airflow:{region}:{account}:environment/etl-mwaa",
                ],
            )
        )
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                sid="MwaaS3GetBucketLocation",
                actions=["s3:GetBucketLocation", "s3:ListAllMyBuckets"],
                resources=["*"],
            )
        )

        # ------------------------------------------------------------------
        # OpenSearch access policy: allow es:ESHttp* from all four IAM roles.
        # PROD: add VPC endpoint and restrict to VPC CIDR instead of IAM ARNs.
        # ------------------------------------------------------------------
        opensearch_domain.add_access_policies(
            iam.PolicyStatement(
                sid="AllowEtlRoles",
                principals=[
                    iam.ArnPrincipal(mwaa_role.role_arn),
                    iam.ArnPrincipal(glue_role.role_arn),
                    iam.ArnPrincipal(lambda_role.role_arn),
                    iam.ArnPrincipal(ec2_role.role_arn),
                ],
                actions=["es:ESHttp*"],
                resources=[f"{opensearch_domain.domain_arn}/*"],
            )
        )

        # ------------------------------------------------------------------
        # 7. AWS Glue Job
        # Type: pythonshell (Python 3.9), 1/16 DPU (0.0625)
        # Script must be uploaded to s3://{dags_bucket}/scripts/etl_glue_job.py
        # before the job can run.
        # PROD: add a VPC connection if the Glue job needs to reach a VPC resource.
        # ------------------------------------------------------------------
        glue_job = glue.CfnJob(
            self,
            "EtlGlueJob",
            name="etl-extraction-job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                python_version="3.9",
                script_location=f"s3://{dags_bucket.bucket_name}/scripts/etl_glue_job.py",
            ),
            glue_version="3.0",
            max_capacity=0.0625,  # 1/16 DPU — minimum for pythonshell
            default_arguments={
                "--run_id": "PLACEHOLDER",
                "--source_path": f"s3://{source_bucket.bucket_name}/",
                "--target_path": f"s3://{target_bucket.bucket_name}/",
                "--partition_date": "PLACEHOLDER",
                # Uncomment to add extra Python libraries from S3:
                # "--extra-py-files": f"s3://{dags_bucket.bucket_name}/libs/etl_libs.zip",
            },
            description="ETL extraction and transformation job (pythonshell, Python 3.9)",
            # PROD: add Connections=[...] if a VPC connection is required.
        )

        # ------------------------------------------------------------------
        # 8. Lambda Function
        # Runtime: Python 3.11, handler: handler.handler
        # Real implementation lives in lambda/handler.py — package it as a zip
        # and replace the inline code below before deploying to production.
        # PROD: use lambda_.Code.from_asset("../lambda") with a proper zip.
        # ------------------------------------------------------------------
        lambda_fn = lambda_.Function(
            self,
            "EtlTransformFunction",
            function_name="etl-transform",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            # Inline placeholder — replace with lambda_.Code.from_asset("../lambda")
            # once the real handler code is ready.
            code=lambda_.Code.from_inline(
                "def handler(event, context):\n"
                "    # TODO: replace with real implementation from lambda/handler.py\n"
                "    return {\"statusCode\": 200, \"body\": \"placeholder\"}\n"
            ),
            role=lambda_role,
            timeout=cdk.Duration.minutes(5),
            memory_size=512,
            description="ETL transform Lambda function",
            environment={
                # PROD: wire these up by reading from Secrets Manager at deploy time
                # or use a Lambda extension to inject secrets at runtime.
                # Do NOT store real secret values here.
                "OPENSEARCH_ENDPOINT": "REPLACE_ME_FROM_SECRET",
                "OPENSEARCH_INDEX": "REPLACE_ME_FROM_SECRET",
            },
        )

        # ------------------------------------------------------------------
        # 9. EC2 Instance (Amazon Linux 2023, SSM-managed, private subnet)
        # No inbound security group rules — access is exclusively via SSM.
        # SSM agent is pre-installed on Amazon Linux 2023.
        # ------------------------------------------------------------------

        # Security group: no inbound rules; allow all outbound (for SSM + S3 + OpenSearch).
        ec2_sg = ec2.SecurityGroup(
            self,
            "Ec2WorkerSg",
            vpc=vpc,
            security_group_name="etl-ec2-worker-sg",
            description="ETL EC2 worker — no inbound rules, SSM access only",
            allow_all_outbound=True,
        )
        # No inbound rules added — SSM does not require open inbound ports.

        # User data: install Python 3 / pip and place the ETL script.
        user_data = ec2.UserData.for_linux()
        user_data.add_commands(
            "#!/bin/bash",
            "set -euxo pipefail",
            "# Update package index and install Python 3 + pip",
            "dnf update -y",
            "dnf install -y python3 python3-pip",
            "# Create the ETL script directory",
            "mkdir -p /opt/etl",
            "# Write a placeholder custom_script.py",
            "# PROD: replace this with a proper S3 download or baked AMI.",
            "cat > /opt/etl/custom_script.py << 'SCRIPT'",
            "#!/usr/bin/env python3",
            "\"\"\"",
            "ETL EC2 custom script placeholder.",
            "Replace with the real implementation before deploying to production.",
            "\"\"\"",
            "import os, sys",
            "run_id      = os.environ.get('RUN_ID', 'unknown')",
            "input_path  = os.environ.get('INPUT_PATH', '')",
            "output_path = os.environ.get('OUTPUT_PATH', '')",
            "print(f'run_id={run_id} input={input_path} output={output_path}')",
            "sys.exit(0)",
            "SCRIPT",
            "chmod +x /opt/etl/custom_script.py",
        )

        ec2_instance = ec2.Instance(
            self,
            "EtlEc2Worker",
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3, ec2.InstanceSize.MEDIUM
            ),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            security_group=ec2_sg,
            # Attach the instance profile created above.
            # CDK's ec2.Instance accepts an iam.Role directly; the instance profile
            # is created automatically.  We pass ec2_role here so CDK wires it up.
            role=ec2_role,
            user_data=user_data,
            instance_name="etl-ec2-worker",
            # SSM agent is pre-installed on Amazon Linux 2023 — no key pair needed.
            key_pair=None,
        )
        # Ensure the instance profile CFN resource is created before the instance.
        ec2_instance.node.add_dependency(ec2_instance_profile)

        # ------------------------------------------------------------------
        # 10. MWAA Environment
        # Dev/test sizing: mw1.small, max 2 workers.
        # PROD: upgrade to mw1.medium or mw1.large and increase max_workers.
        # ------------------------------------------------------------------

        # Security group for MWAA: allow HTTPS outbound (443) for provider calls.
        mwaa_sg = ec2.SecurityGroup(
            self,
            "MwaaSg",
            vpc=vpc,
            security_group_name="etl-mwaa-sg",
            description="MWAA environment security group — HTTPS outbound only",
            allow_all_outbound=False,
        )
        mwaa_sg.add_egress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(443),
            description="Allow HTTPS outbound for AWS API calls",
        )
        # MWAA also needs port 5432 (Airflow metadata DB) — managed internally by
        # the service, but the SG must allow self-referencing traffic.
        mwaa_sg.add_ingress_rule(
            peer=mwaa_sg,
            connection=ec2.Port.all_traffic(),
            description="Allow intra-MWAA traffic (scheduler <-> worker)",
        )
        mwaa_sg.add_egress_rule(
            peer=mwaa_sg,
            connection=ec2.Port.all_traffic(),
            description="Allow intra-MWAA traffic (scheduler <-> worker)",
        )

        # Collect private subnet IDs for the MWAA network configuration.
        private_subnet_ids = [
            subnet.subnet_id
            for subnet in vpc.private_subnets
        ]

        mwaa_env = mwaa.CfnEnvironment(
            self,
            "EtlMwaaEnvironment",
            name="etl-mwaa",
            airflow_version="2.9.2",
            source_bucket_arn=dags_bucket.bucket_arn,
            dag_s3_path="dags/",
            plugins_s3_path="plugins/",
            requirements_s3_path="requirements.txt",
            execution_role_arn=mwaa_role.role_arn,
            # Dev/test sizing — PROD: change to mw1.medium or mw1.large
            environment_class="mw1.small",
            max_workers=2,
            min_workers=1,
            schedulers=2,
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                subnet_ids=private_subnet_ids,
                security_group_ids=[mwaa_sg.security_group_id],
            ),
            logging_configuration=mwaa.CfnEnvironment.LoggingConfigurationProperty(
                dag_processing_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO"
                ),
                scheduler_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO"
                ),
                task_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO"
                ),
                webserver_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO"
                ),
                worker_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO"
                ),
            ),
            airflow_configuration_options={
                "core.load_examples": "false",
                "webserver.dag_default_view": "graph",
            },
            webserver_access_mode="PUBLIC_ONLY",
            # PROD: change to PRIVATE_ONLY and set up a VPN or Direct Connect.
        )
        # MWAA depends on the DAGs bucket and execution role being ready.
        mwaa_env.node.add_dependency(dags_bucket)
        mwaa_env.node.add_dependency(mwaa_role)

        # ------------------------------------------------------------------
        # 11. CloudFormation Outputs
        # ------------------------------------------------------------------
        CfnOutput(
            self,
            "OutputMwaaEnvironmentName",
            value=mwaa_env.name,
            export_name="MwaaEnvironmentName",
            description="Name of the MWAA Airflow environment",
        )
        CfnOutput(
            self,
            "OutputOpenSearchDomainEndpoint",
            value=opensearch_domain.domain_endpoint,
            export_name="OpenSearchDomainEndpoint",
            description="OpenSearch domain endpoint (HTTPS)",
        )
        CfnOutput(
            self,
            "OutputDagsBucketName",
            value=dags_bucket.bucket_name,
            export_name="DagsBucketName",
            description="S3 bucket that stores MWAA DAG files and plugins",
        )
        CfnOutput(
            self,
            "OutputSnsTopicArn",
            value=failure_topic.topic_arn,
            export_name="SnsTopicArn",
            description="SNS topic ARN for ETL pipeline failure notifications",
        )
        CfnOutput(
            self,
            "OutputGlueJobName",
            value=glue_job.name,  # type: ignore[arg-type]
            export_name="GlueJobName",
            description="Name of the Glue extraction job",
        )
        CfnOutput(
            self,
            "OutputLambdaFunctionName",
            value=lambda_fn.function_name,
            export_name="LambdaFunctionName",
            description="Name of the ETL transform Lambda function",
        )
        CfnOutput(
            self,
            "OutputEc2InstanceId",
            value=ec2_instance.instance_id,
            export_name="Ec2InstanceId",
            description="EC2 instance ID of the ETL worker",
        )
