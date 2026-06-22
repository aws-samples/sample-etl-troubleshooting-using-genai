---
title : "Setup"
weight : 20
---

In this workshop, whether you are part of an AWS event or using your own account, you will deploy a set of CloudFormation templates to set up the resources that are required to build the conversational ETL Observability Tool:

* Amazon OpenSearch Service cluster
* Amazon SageMaker Jupyter Notebooks
* Amazon Managed Workflows for Apache Airflow (MWAA)
* AWS Glue
* Amazon EC2
* IAM roles

## At an AWS Event

If you are at an AWS HOSTED EVENT (AWS re\:Invent, AWS Summit, Immersion day etc.) you may be provided a temporary AWS account with the workshop resources already created. Please follow the instructions for an [AWS event](./workshopatawsevent) to get started.

## Using your own AWS Account

If you are running this workshop by yourself, your account must have the ability to create new IAM roles and scope other IAM permissions. Please follow the [AWS account instructions](./use-own-aws-account) for using your own account.