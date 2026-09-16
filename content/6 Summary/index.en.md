---
title : "Summary"
weight : 90
---
## Summary
This solution demonstrates how organizations can enhance the granularity of their data pipeline observability by combining Amazon OpenSearch Service with Large Language Models through Amazon Bedrock. By streaming logs from MWAA, AWS Glue, and EC2 into OpenSearch via CloudWatch Subscription Filters, teams gain a unified view of their entire pipeline execution without data duplication. The integration of the OpenSearch ML Connector with Bedrock enables semantic search and natural language querying, transforming how engineers interact with operational logs, from manual log searching to conversational AI-driven error detection and remediation.
The key benefits of this architecture include real-time log aggregation without ETL overhead, granular cross-service observability that correlates errors across orchestration and compute layers, and an AI-powered interface that reduces mean time to resolution.

## What you accomplished
- Deployed a multi-service ETL pipeline with MWAA orchestration
- Consolidated logs from disparate sources into a single OpenSearch index
- Connected an AI agent to OpenSearch via the Model Context Protocol
- Queried pipeline logs using natural language from a SageMaker Notebook
- Explored both local development and production deployment patterns for AI agents
## How this benefits you
- Faster incident response: Instead of correlating timestamps across four log groups, ask one question and get a synthesized answer in seconds.
- Lower barrier to entry: Team members who don't know OpenSearch DSL can still perform sophisticated log analysis by asking questions in plain English.
- Pattern recognition at scale: The AI agent can identify correlations across services that would take a human much longer to spot manually.
- Reusable architecture: The MCP server pattern works with any OpenSearch index. You can apply this same approach to application logs, security events, or business metrics.