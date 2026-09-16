---
title : "Clean Up"
weight : 100
---

This module will help you clean up the resources you have deployed in your AWS account during this lab.

To get started, follow the instructions below:

## At an AWS event

::alert[If you're at an AWS event, you may skip this section. AWS will clean up the resources automatically from the accounts after the event is completed. Please do not use this account for any other purposes than stated in the event and in the labs.]{header="Important"}

## Cleaning up resources from your own AWS account

This workshop deploys three CloudFormation stacks. You must delete all three to stop incurring charges. Delete them in reverse order to avoid dependency errors.

### Step 1: Empty S3 buckets

CloudFormation cannot delete S3 buckets that contain objects. You need to empty the buckets from both the `opensearch-cfn` and `etl` stacks before deleting them.

1. Go to the S3 console by typing S3 in the top search bar and click the Amazon S3 from the search results.

:image[S3 console navigation from the AWS search bar]{src="/static/s3-navigate.png"}

2. Search for buckets from each stack. For each bucket found, select it and click **Empty**.

:image[Selecting an S3 bucket and clicking the Empty button]{src="/static/s3-select-empty-click.png"}

3. When prompted, enter **permanently delete** in the text box to empty the S3 bucket.

:image[Confirmation dialog requiring permanently delete text entry]{src="/static/permanent-delete.png"}

:::alert{header="Important" type="warning"}
Any data contained in the bucket will be deleted permanently. Please make sure you do not have any data that you will need in the future.
:::

4. Confirm there are no objects in the **Failed to delete** section. Click **Exit.**

:image[Empty bucket confirmation screen showing successful deletion]{src="/static/empty-bucket-screen.png"}

5. Repeat for all S3 buckets created by the workshop (look for buckets containing `opensearch-cfn` or `etl` in the name).

### Step 2: Delete CloudFormation stacks

Delete the stacks in this order:

1. Navigate to the AWS CloudFormation console.

:image[Navigating to CloudFormation from the AWS search bar]{src="/static/navigate-cfn.png"}

2. Select the **agentcore-mcp-server** stack. Click **Delete**.

:image[Selecting a CloudFormation stack and clicking the Delete button]{src="/static/select-stack-delete.png"}

3. Confirm deletion.

:image[CloudFormation delete confirmation dialog]{src="/static/delete-confirm.png"}

4. Wait for the stack to reach DELETE_COMPLETE status.

5. Repeat for the **etl** stack. Wait for DELETE_COMPLETE.

6. Finally, delete the **opensearch-cfn** stack.

:::alert{header="Important" type="warning"}
The `opensearch-cfn` stack must be deleted last because the `agentcore-mcp-server` stack imports values from it. Deleting out of order will cause dependency errors.
:::

7. Once all three stacks are deleted, all workshop resources have been removed and charges will stop.