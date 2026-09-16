---
title: "Monitoring MWAA-Orchestrated ETL Pipelines with Amazon OpenSearch"
weight: 0
---

When an ETL pipeline spans multiple AWS services — Glue jobs transforming data, EC2 scripts running custom logic, and Airflow DAGs coordinating it all, troubleshooting a failure means hunting through multiple CloudWatch logs. When something breaks at 2 AM, you don't want to have to spend the first 30 minutes just finding the right log stream.

This workshop demonstrates how Generative AI can dramatically improve error analysis of ETL jobs orchestrated by Amazon Managed Workflows for Apache Airflow (MWAA). By consolidating pipeline logs into Amazon OpenSearch Service and layering an AI agent on top, you can query those logs in plain English — asking questions like "what errors occurred in the last Glue job run?" and getting immediate, contextual answers.

## Target audience

This workshop is designed for data engineers, DevOps engineers, and cloud architects who build or operate ETL pipelines on AWS. Basic familiarity with the AWS Console and Python is assumed.

## Prerequisites

- An AWS account with permissions to create CloudFormation stacks, IAM roles, and the services used in this workshop
- Familiarity with the AWS Console
- Basic Python knowledge (the notebook code is provided, but understanding it helps)
- A modern web browser

## Supported regions

This workshop can be deployed in **us-west-2** (Oregon) or **us-east-1** (N. Virginia). All services used (OpenSearch, MWAA, Bedrock AgentCore, SageMaker) are available in these regions.

## Expected duration

Approximately 90 minutes (60 minutes for guided notebook execution, 30 minutes for exploration and cleanup).

## Cost warning

This workshop creates resources that incur costs. The primary cost drivers are the Amazon OpenSearch Service domain (~$1.50/hr for r6g.2xlarge), the MWAA environment (~$0.49/hr), the SageMaker Notebook instance (~$0.58/hr for ml.m5d.2xlarge), and a NAT Gateway (~$0.045/hr). Estimated total cost is approximately $3-5/hr while running. Be sure to delete all three CloudFormation stacks when finished to stop charges. See the [Clean Up](/6-clean-up/) section for instructions.

## Learning outcomes

By the end of this workshop, you will have:
- A deployed ETL pipeline generating logs across AWS Glue, EC2, and MWAA
- A Lambda-based pipeline streaming CloudWatch logs into OpenSearch in near real-time
- A Claude LLM connector registered in OpenSearch for intelligent analysis
- An AI agent (via Strands Agents + MCP) that can search, correlate, and explain errors across all your ETL logs in plain English

## Objectives

In this lab you will learn how to:
- Deploy an observable ETL pipeline orchestrated by MWAA
- Consolidate logs from AWS Glue, EC2, and MWAA into Amazon OpenSearch
- Use the OpenSearch MCP (Model Context Protocol) server to enable AI-powered log analysis
- Interact with the GenAI solution from SageMaker AI Notebooks using the Strands Agents framework

## Deployment overview

The solution consists of three CloudFormation stacks:
### Stack 1: Vector search with RAG using OpenSearch
Provisions the foundational OpenSearch domain along with a SageMaker Notebook instance for interactive exploration. This stack creates:
- Amazon OpenSearch Service domain (OpenSearch 3.5) with fine-grained access control
- SageMaker Notebook instance pre-loaded with workshop lab notebooks
- S3 buckets for ETL data, DAGs, ect
- IAM roles for notebook and Bedrock access
- Secrets Manager secret for OpenSearch credentials
### Stack 2: OpenSearch MCP server
Deploys an AI agent runtime using Amazon Bedrock AgentCore that exposes OpenSearch as a set of tools via the Model Context Protocol. This stack creates:
- Amazon Bedrock AgentCore runtime hosting the OpenSearch MCP server
- Cognito User Pool for OAuth authentication
- ECR repository for the MCP server container
- CodeBuild project to build and deploy the container
### Stack 3: Observability ETL pipeline
Provisions the ETL infrastructure that generates the logs you will analyze. This stack creates:
- VPC with private subnets and NAT Gateway for MWAA networking
- Amazon MWAA environment running an Airflow DAG
- AWS Glue ETL job (reads XLSX, drops a column, writes JSON)
- EC2 instance running a parallel Python ETL script
- MWAA Serverless workflow for aggregation
- S3 buckets for DAG storage and ETL data
