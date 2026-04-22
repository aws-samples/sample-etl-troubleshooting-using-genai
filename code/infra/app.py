#!/usr/bin/env python3
"""
CDK App entry point for the MWAA ETL Workflow infrastructure stack.

Account and region are resolved in priority order:
  1. CDK context keys ``account`` / ``region`` (pass with --context or in cdk.json)
  2. Environment variables ``CDK_DEFAULT_ACCOUNT`` / ``CDK_DEFAULT_REGION``
     (set automatically by the CDK CLI when you run ``cdk deploy``)
  3. CDK's pseudo-references (resolved at synthesis time from the caller's
     AWS credentials / config).

Usage:
    cd infra/
    cdk deploy                          # uses current AWS profile
    cdk deploy --context account=123456789012 --context region=us-east-1
"""

import os

import aws_cdk as cdk

from mwaa_etl_stack import MwaaEtlStack

app = cdk.App()

# Resolve account and region from context, then env vars, then CDK defaults.
account = (
    app.node.try_get_context("account")
    or os.environ.get("CDK_DEFAULT_ACCOUNT")
    or os.environ.get("AWS_ACCOUNT_ID")
)
region = (
    app.node.try_get_context("region")
    or os.environ.get("CDK_DEFAULT_REGION")
    or os.environ.get("AWS_DEFAULT_REGION")
    or "us-east-1"
)

MwaaEtlStack(
    app,
    "MwaaEtlStack",
    env=cdk.Environment(account=account, region=region),
    # Stack-level description shown in the CloudFormation console.
    description=(
        "MWAA ETL Workflow: VPC, S3, IAM, Secrets Manager, SNS, OpenSearch, "
        "Glue, Lambda, EC2, and MWAA environment for the ETL pipeline."
    ),
)

app.synth()
