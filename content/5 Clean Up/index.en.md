---
title : "Clean Up"
weight : 100
---

This module will help you clean up the resources you have deployed in your AWS account during this lab.

To get started, follow the instructions below:

## At an AWS event

::alert[If you're at an AWS event, you may skip this section. AWS will clean up the resources automatically from the accounts after the event is completed. Please do not use this account for any other purposes than stated in the event and in the labs.]{header="Important"}

## Cleaning up resources from your own AWS account

1. Go to the S3 console by typing S3 in the top search bar and click the Amazon S3 from the search results.

:image[s3navigate]{src="/static/s3-navigate.png"}

2. Type in the name of the stack in the search bar in **General purpose buckets** section. Select the bucket with a name that starts with the CloudFormation stack that we deployed and contains **s3buckettraining** in the name. Click **Empty** after selecting the bucket.

:image[s3empty]{src="/static/s3-select-empty-click.png"}

3. When prompted, please enter **permanently delete** in the text box to empty the S3 bucket.

:image[permanentlydelete]{src="/static/permanent-delete.png"}

:::alert{header="Important" type="warning"}
Please note that any data contained in the bucket will be deleted permanently. Please make sure you do not have any data that you will need in the future. There will be no backup of this data once deleted.

:::

4. You should see a screen that confirms that all data is deleted. Please make sure there is no object mentioned in the **Failed to delete** section. Click **Exit.**

:image[emptybucketscreen]{src="/static/empty-bucket-screen.png"}

5. Navigate to the AWS CloudFormation console by searching CloudFormation in the top bar and select **AWS CloudFormation** from the search results.

:image[navigatecfn]{src="/static/navigate-cfn.png"}

6. Select the name of the stack that we deployed at the beginning of the workshop. Click the **Delete** button from the top right.

:image[selectstackdelete]{src="/static/select-stack-delete.png"}

7. On the delete confirmation dialog, please click **Delete** to proceed with deleting the stack.

:image[delete-confirm]{src="/static/delete-confirm.png"}

8. Once deleted, all the resources that were created as part of the workshop will have been deleted.

:::alert{header="Important" type="warning"}
Once deleted, you will not have access to the OpenSearch dashboards or SageMaker notebook that you used in the workshop. Please make sure your account does not have any resources left to be deleted and delete them as necessary.

::: 