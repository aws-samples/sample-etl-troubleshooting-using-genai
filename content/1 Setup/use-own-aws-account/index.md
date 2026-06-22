---
title : "Using your own AWS account"
weight : 26
---


::alert[You can skip this section if you are at an AWS hosted event and provided with AWS accounts.]{header="Important"}
If you're NOT running these labs as part of an AWS led event in Workshop Studio, you will need to prepare the environment before you can complete these labs. Please note that these resources will incur a cost, so remember to clean up these resources when you have finished.

To configure your account, complete the following steps.

## Step 1: Deploy the OpenSearch Infrastructure

1. Click the Launch Stack button below to launch CloudFormation with some pre-configured values in the **us-east-1** region. Please remember to change the region if you would like to run this workshop in a different region. Please note this workshop has been tested in **us-east-1**. Please ensure Amazon Bedrock service is available in the region you're deploying this stack to.

<!--:button[Launch Multimodal Stack]{href="https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/review?templateURL=https://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee.s3.us-east-1.amazonaws.com/cfda120e-98ef-49df-b875-6f0f33e99615/opensearch_cfn.yaml&stackName=opensearch-cfn" variant="primary"}-->

:button[Launch Multimodal Stack]{href="https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/create/review?templateURL=https://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee.s3.us-east-1.amazonaws.com/cfda120e-98ef-49df-b875-6f0f33e99615/opensearch_cfn.yaml&stackName=opensearch-cfn" variant="primary"}


2. On the **Quick create stack** page, note that the S3 URL and stack name is already set to `opensearch-cfn`. Mark the checkbox next to **I acknowledge that AWS CloudFormation might create IAM resources,** and click **Create stack.**

3. Wait for the stack to complete. This may take 15-20 minutes to complete as it initiates an Amazon OpenSearch Service instance. Wait until the status of the stack changes to **CREATE_COMPLETE.**

## Step 2: Deploy the AgentCore MCP Server

1. After the OpenSearch stack completes, click the Launch Stack button below to deploy the AgentCore MCP Server:

<!-- :button[Launch AgentCore Stack]{href="https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/review?templateURL=https://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee.s3.us-east-1.amazonaws.com/cfda120e-98ef-49df-b875-6f0f33e99615/agentcore-mcp-server.yaml&stackName=agentcore-mcp-server" variant="primary"} -->

:button[Launch AgentCore Stack]{href="https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/create/review?templateURL=https://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee.s3.us-east-1.amazonaws.com/cfda120e-98ef-49df-b875-6f0f33e99615/agentcore-mcp-server.yaml&stackName=agentcore-mcp-server" variant="primary"}

2. On the **Quick create stack** page:
   - Ensure the **MultimodalStackName** parameter is set to `opensearch-cfn` (matching the first stack name)
   - Configure other parameters as needed
   - Mark the checkbox next to **I acknowledge that AWS CloudFormation might create IAM resources**
   - Click **Create stack**

3. Wait for this stack to complete (typically 5-10 minutes).

## Step 3: Deploy the ETL Resources

. After the Agent Core stack completes, click the Launch Stack button below to deploy the ETL resources:

<!-- :button[Launch AgentCore Stack]{href="https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/review?templateURL=https://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee.s3.us-east-1.amazonaws.com/cfda120e-98ef-49df-b875-6f0f33e99615/agentcore-mcp-server.yaml&stackName=agentcore-mcp-server" variant="primary"} -->

:button[Launch AgentCore Stack]{href="https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/create/review?templateURL=https://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee.s3.us-east-1.amazonaws.com/cfda120e-98ef-49df-b875-6f0f33e99615/agentcore-mcp-server.yaml&stackName=agentcore-mcp-server" variant="primary"}

5. On the **Quick create stack** page:
   - Ensure the **MultimodalStackName** parameter is set to `opensearch-cfn` (matching the first stack name)
   - Configure other parameters as needed
   - Mark the checkbox next to **I acknowledge that AWS CloudFormation might create IAM resources**
   - Click **Create stack**

6. Wait for this stack to complete (typically 5-10 minutes).

## Access Your Resources

7. Once both stacks are complete, click on the **Outputs** tab of the `opensearch-cfn` stack. These output values will be used later in this workshop. Click the URL link next to **SageMakerNotebookURL** to open Amazon SageMaker in the AWS Management Console.

8. For the AgentCore functionality, check the **Outputs** tab of the `agentcore-mcp-server` stack for the MCP server endpoint and other relevant URLs.

## Clean up

Once you are finished with the workshop, you should clean up the resources to avoid incurring costs. Delete both CloudFormation stacks in reverse order:
1. First delete the `agentcore-mcp-server` stack
2. Then delete the `opensearch-cfn` stack
3. Finally delete the `etl` stack

You can find detailed cleanup instructions in the Clean up section.