---
title : "Notebook Overview"
weight : 40
---
## Overview

You will be running this workshop using an Amazon SageMaker AI Notebook. This Notebook builds an end-to-end observability solution for ETL workloads orchestrated by Amazon Managed Workflows for Apache Airflow (MWAA). It connects CloudWatch logs from multiple AWS services — EC2, MWAA, and AWS Glue — into Amazon OpenSearch Service, then uses an AI agent powered by the Model Context Protocol (MCP) to query those logs using natural language to identify and errors in the ETL workloads and provide remediation guidance.

By the end of the notebook, you will have:

- An ETL DAG running and generating logs across your infrastructure
- A Lambda-based pipeline streaming CloudWatch logs into OpenSearch in near real-time
- A Claude LLM connector registered in OpenSearch for intelligent analysis
- An AI agent (via Strands Agents + MCP) that can search, correlate, and explain errors across all your ETL logs in plain English

## Architecture

![Architecture diagram showing CloudWatch logs from EC2, MWAA, and Glue streaming into OpenSearch via Lambda, with an AI agent querying logs through the MCP server](/static/architecture/architecture-diagram.png)

## Notebook explanation

**NOTE:** These explanations and notebook execution guidance are all documented in the notebook itself.  They are provided below as a reference. You can go perform all work in the notebook and are not required to follow these while doing so. To continue with the lab please go to the ***Running the Workshop*** section listed in the left hand menu.

### 0. Open the notebook
If you are particiapting in an AWS hosted workshop, you can find the link to the SageMaker AI Notebook and other resources used in the workshop by clicking on the name of the lab in the upper left hand corner.  Click on the link to the SageMaker AI Notebook.

![Workshop Studio event outputs panel showing the SageMaker AI Notebook URL link](/static/notebook_overview/workshop-notebooklink.png)

**NOTE:** You may need to open the AWS Console before opening the SageMaker AI Notebook. This is to make sure that the workshop credentials are used to access the notebook.

If you are deploying this in your AWS account, you can find the link to the SageMaker AI Notebook and other the resources resources created by the CloudFormation scripts by:
1. Going to CloudFormation in your AWS Console
2. Selecting the opensearch-cfn stack

![CloudFormation console showing the opensearch-cfn stack selected](/static/notebook_overview/cfn-stacks.png)

3. Selecting the **Outputs** tab and then scrolling down and clicking the URL for the SageMaker AI Notebook

![CloudFormation Outputs tab showing the SageMaker AI Notebook URL](/static/notebook_overview/cfn-stacks-output.png)

### 1. Complete prerequisites
**Section 1** of the notebook loads the required python modules to execute the code in this notebook. It also retrieves resource metadata for the resources created by the CloudFormation stacks. These cells need to be run before moving on to **Section 2**.

### 2. Connect to OpenSearch
In **Section 2** you programmatically connect to OpenSearch so that you can interact with the OpenSearch index from the notebook.
1. **Retrieve credentials** from AWS Secrets Manager (internal admin username/password).
2. **Create an OpenSearch client** using those credentials.
3. **Map IAM roles** (notebook role + AgentCore execution role) to the `all_access` OpenSearch backend role.
4. **Switch to IAM (SigV4) authentication** for all subsequent API calls. This cell can be re-run to refresh expired tokens.
5. **Persist connection variables** (`%store`) for use in later cells or after kernel restarts.

### 3. Stream CloudWatch logs to OpenSearch
In **Section 3** you create a Lambda function and CloudWatch subscription filters for the ETL related log groups. Lambda is used to connect the subscription filter to OpenSearch.
1. **Discover log groups** by scanning prefixes that cover EC2 ETL, Airflow, Glue, MWAA Serverless, and OpenSearch logs.
2. **Create a Lambda execution IAM role** with permissions to write to the OpenSearch domain and create CloudWatch log streams.
3. **Deploy a Lambda function** that:
   - Receives CloudWatch subscription filter events
   - Decompresses and parses the log payload
   - Signs requests with SigV4
   - Bulk-indexes documents into date-stamped `cwl-YYYY.MM.DD` indices
4. **Grant CloudWatch Logs permission** to invoke the Lambda for each discovered log group.
5. **Create subscription filters** on all target log groups pointing at the Lambda.
6. **Add the Lambda role to OpenSearch `all_access`** so it can write to indices.
7. **Verify ingestion** by querying today's `cwl-*` index for document count.

### 4. Trigger the MWAA DAG
In **Section 4** you run the DAG that executes the example ETL pipeline. The DAG needs to be run so that there are logs in CloudWatch.
Two modes are supported:
- **Provisioned MWAA** (default) — triggers `observability_etl_dag` which runs Glue and EC2 tasks in parallel.
- **MWAA Serverless** — triggers `observability_blog_aggregation` for Glue aggregation.

### 5. Register Claude LLM connector in OpenSearch
1. **Create an ML connector** in OpenSearch that calls Amazon Bedrock's Claude model (`us.anthropic.claude-sonnet-4-6`) via the Converse API using SigV4 authentication and an assumed IAM role.
2. **Register and deploy the model** so OpenSearch can use it for ML-powered features (RAG, conversational search, etc.).

### 6. AI agent with OpenSearch MCP server
1. **Install agent libraries** (`mcp`, `strands-agents`, `uv`).
2. **Load Cognito credentials** for authenticating with the AgentCore MCP server.
3. **Choose a deployment mode:**
   - **Option A (Local)** — runs the OpenSearch MCP server as a subprocess via `uvx` for development.
   - **Option B (AgentCore)** — connects to a production MCP server on Amazon Bedrock AgentCore using OAuth 2.0 client credentials.
4. **Create specialized agents:**
   - **DevOps Agent** — answers cluster health and shard questions (uses `ClusterHealthTool`, `GetShardsTool`).
   - **Search Agent** — discovers and analyzes ETL logs for errors across Glue, MWAA, DAG, and EC2 log groups (uses `ListIndexTool`, `IndexMappingTool`, `SearchIndexTool`, etc.).
5. **Query logs in natural language** — ask questions like "What errors do you see in my logs?" and the agent autonomously searches indices, correlates events, and returns structured analysis with remediation guidance.