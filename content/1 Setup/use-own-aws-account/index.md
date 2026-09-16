---
title : "Using your own AWS account"
weight : 26
---


::alert[You can skip this section if you are at an AWS hosted event and provided with AWS accounts.]{header="Important"}
If you're NOT running these labs as part of an AWS led event in Workshop Studio, you will need to prepare the environment before you can complete these labs. Please note that these resources will incur a cost, so remember to clean up these resources when you have finished.

To configure your account, complete the following steps.

## Deploy the infrastructure
1. Download the [Opensearch_cfn](https://ws-assets-prod-iad-r-pdx-f3b3f9f1a7d6a3d0.s3.us-west-2.amazonaws.com/8b5c9a36-6874-4f13-a4a2-b1bff1363694/opensearch_cfn.yaml) stack
2. Go to the AWS Console, then the CloudFormation console.  
3. Select **Create stack** in the upper right hand corner and then select **With new resources (standard)**
![CloudFormation console showing the Create stack button with the With new resources option highlighted](/static/cfn/cfn01.png)
4. Select the toggle for ***Upload a template file***
5. Select **Choose file** and then select the Opensearch_cfn.yaml file that you downloaded.
6. Select **Next** then use ***opensearch-cfn*** as the Stack name, leave the other fields at their defaults.  Select **Next**
![CloudFormation stack creation form with opensearch-cfn entered as the stack name](/static/cfn/cfn02.png)
7. Leave every as default and then checkmark ***I acknowledge that CloudFormation might create IAM resources***. Select **Next**.
![CloudFormation configuration page with the IAM resources acknowledgment checkbox checked](/static/cfn/cfn03.png)
8. Click **Submit** and then wait for the stack to complete and the status of the stack changes to ***CREATE_COMPLETE***.
9. Download the [ETL](https://ws-assets-prod-iad-r-pdx-f3b3f9f1a7d6a3d0.s3.us-west-2.amazonaws.com/8b5c9a36-6874-4f13-a4a2-b1bff1363694/etl.yaml) and [Agentcore-mcp-server](https://ws-assets-prod-iad-r-pdx-f3b3f9f1a7d6a3d0.s3.us-west-2.amazonaws.com/8b5c9a36-6874-4f13-a4a2-b1bff1363694/agentcore-mcp-server.yaml) stacks.
10. Deploy the ETL CloudFormation stack the same way. Set the stack name as etl. Leave all parameters as default. Wait for the stack to complete until the status of the stack changes to CREATE_COMPLETE.
11. Deploy the Agentcore-mcp-server stack the same way. Name it agentcore-mcp-server. Leave all parameters as defaults. Wait for the stack to complete until the status of the stack changes to CREATE_COMPLETE.


## Access your resources

1. Once all stacks are complete, click on the **Outputs** tab of the `opensearch-cfn` stack. These output values will be used later in this workshop. Click the URL link next to **SageMakerNotebookURL** to open Amazon SageMaker in the AWS Management Console.

2. For the AgentCore functionality, check the **Outputs** tab of the `agentcore-mcp-server` stack for the MCP server endpoint and other relevant URLs.

## Clean up

Once you are finished with the workshop, you should clean up the resources to avoid incurring costs. Delete all three CloudFormation stacks in reverse order:
1. First delete the `agentcore-mcp-server` stack
2. Then delete the `etl` stack
3. Finally delete the `opensearch-cfn` stack

You can find detailed cleanup instructions in the Clean up section.