---
title : "Oveview"
weight : 30
---
## Workshop Overview 
This solution creates an ETL observability layer that aggregates logs from all pipeline components and makes them queryable through an OpenSearch MCP server. The architecture connects five key AWS services in a streamlined data flow that eliminates data duplication while enabling powerful AI-driven log analysis. This solution reduces the time needed to identify what caused an error in an ETL pipeline and remediating it.

### End-to-End Data Flow 
The architecture begins with Amazon MWAA as the orchestration layer. An Airflow DAG defines the pipeline workflow, triggering AWS Glue ETL jobs as well as Python scripts running on EC2 instances. Each of these components generates logs that flow into Amazon CloudWatch Logs. MWAA through its native integration, Glue through its default log group configuration, and EC2 through the unified CloudWatch Agent.

### Real-Time Log Streaming
CloudWatch Subscription Filters provide the bridge between log storage and analysis. When configured, these filters immediately start streaming real-time log data from selected log groups to Amazon OpenSearch Service. This approach means that data does not need to be copied or duplicated, the subscription filter creates a real-time streaming pipeline that indexes logs as they arrive.

### AI-Powered Analysis
Within OpenSearch, the ML Connector framework integrates with Amazon Bedrock to enable LLM-based inference over the log indices. The OpenSearch MCP (Model Context Protocol) Server then exposes these capabilities to AI assistants, allowing users to query their pipeline logs using natural language to identify errors, understand failure patterns, and receive contextual remediation suggestions.

![Architecture Diagram](/static/architecture/architecture-diagram.png)

