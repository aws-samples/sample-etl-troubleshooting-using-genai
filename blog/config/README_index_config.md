# OpenSearch Index Configuration

This directory contains two JSON configuration files for the `etl-logs-*` OpenSearch indices used by the MWAA + OpenSearch monitoring solution.

| File | Purpose |
|---|---|
| `index_mapping.json` | Explicit field type mapping for all `Log_Event` fields |
| `ism_policy.json` | Index State Management policy — 30-day retention with automatic deletion |

---

## index_mapping.json

### Why explicit mappings?

OpenSearch can infer field types automatically (dynamic mapping), but relying on inference for a structured log schema creates two problems:

1. **Type conflicts**: If the first document indexed has a `null` for an optional field, OpenSearch may infer the wrong type when a real value arrives later.
2. **Unexpected `text` fields**: OpenSearch defaults string fields to `text` with a nested `keyword` sub-field. For identifier fields that are only ever used for exact-match filtering and aggregations, this wastes storage and slows aggregations.

The mapping uses `"dynamic": "strict"` to reject any document that contains a field not declared in the mapping. This prevents schema drift and catches bugs in the `LogEvent` dataclass early.

### Field type rationale

#### `keyword` fields

`keyword` is used for all identifier and enumeration fields. `keyword` fields are stored as-is (no tokenisation or analysis), which means:

- **Exact-match `term` queries** work correctly — `{ "term": { "run_id": "scheduled__2024-03-15T10:00:00+00:00" } }` returns only documents with that exact value.
- **`terms` aggregations** (e.g., count events per `component_type`) are efficient because OpenSearch can use doc values rather than scanning the inverted index.
- **Sorting** on `keyword` fields is supported without additional configuration.

Fields mapped as `keyword`:

| Field | Reason |
|---|---|
| `run_id` | Correlation key — always queried with exact match to retrieve all events for a single DAG run |
| `task_name` | Airflow task ID — filtered and aggregated by exact value |
| `component_type` | Enumeration (`glue`, `lambda`, `ec2`, `mwaa`) — used in `terms` aggregations and filters |
| `log_level` | Enumeration (`INFO`, `WARN`, `ERROR`) — filtered by exact value in alert monitors |
| `job_name` | Glue job name — exact-match lookup when investigating a specific job |
| `job_run_id` | Glue job run ID — exact-match lookup to correlate with Glue console |
| `terminal_status` | Glue terminal status (`SUCCEEDED`, `FAILED`, `STOPPED`) — filtered in dashboards |
| `function_name` | Lambda function name — exact-match lookup |
| `request_id` | Lambda request ID — exact-match lookup to correlate with CloudWatch Logs |
| `outcome` | Lambda outcome (`success`, `error`) — filtered in dashboards |
| `instance_id` | EC2 instance ID — exact-match lookup |
| `script_name` | Script filename — exact-match lookup |

#### `text` fields

`text` is used only for `message` because it is the only field that benefits from full-text search (tokenisation, stemming, relevance scoring). Readers may want to search for keywords within log messages (e.g., "connection refused") rather than matching the entire string exactly.

| Field | Reason |
|---|---|
| `message` | Human-readable description — full-text search is useful for ad-hoc investigation |

#### `date` fields

`date` fields are stored as epoch milliseconds internally, which enables efficient range queries and time-series aggregations in OpenSearch Dashboards. All three date fields accept ISO 8601 UTC strings (e.g., `"2024-03-15T10:30:00Z"`).

| Field | Reason |
|---|---|
| `timestamp` | Primary time field — used for all time-range filters and the Dashboards time picker |
| `start_time` | Task start time — used to compute duration and for time-range analysis |
| `end_time` | Task end time — used to compute duration and for time-range analysis |

#### `long` fields

`long` (64-bit integer) is used for `duration_ms` because task durations can exceed the range of a 32-bit integer for long-running Glue jobs (e.g., a 25-hour job produces `duration_ms = 90_000_000`).

| Field | Reason |
|---|---|
| `duration_ms` | Duration in milliseconds — `long` accommodates multi-hour jobs without overflow |

#### `integer` fields

`integer` (32-bit signed integer) is sufficient for `exit_code` because POSIX exit codes are in the range 0–255.

| Field | Reason |
|---|---|
| `exit_code` | Script exit code — small integer, `integer` type is sufficient |

---

## ism_policy.json

The ISM policy automates index lifecycle management for all `etl-logs-*` indices. It defines two states:

- **`hot`**: The active state. Indices in this state accept writes and serve queries. After 30 days, the index transitions to `delete`.
- **`delete`**: The terminal state. OpenSearch deletes the index immediately upon entering this state.

The `ism_template` block with `index_patterns: ["etl-logs-*"]` and `priority: 100` ensures the policy is automatically attached to every new index whose name matches the pattern. No manual policy attachment is needed when a new monthly index is created.

---

## Applying the configuration via AWS CLI

Replace `<OPENSEARCH_ENDPOINT>` with your domain endpoint (without a trailing slash), and `<YYYY-MM>` with the current year and month (e.g., `2024-03`).

### 1. Create the index with the mapping

```bash
curl -XPUT \
  -H "Content-Type: application/json" \
  -d @index_mapping.json \
  "https://<OPENSEARCH_ENDPOINT>/etl-logs-<YYYY-MM>"
```

Or using the AWS CLI with SigV4 signing (requires `awscurl`):

```bash
awscurl --service es --region <AWS_REGION> \
  -X PUT \
  -H "Content-Type: application/json" \
  -d @index_mapping.json \
  "https://<OPENSEARCH_ENDPOINT>/etl-logs-<YYYY-MM>"
```

Expected response:

```json
{"acknowledged": true, "shards_acknowledged": true, "index": "etl-logs-2024-03"}
```

### 2. Create the ISM policy

```bash
awscurl --service es --region <AWS_REGION> \
  -X PUT \
  -H "Content-Type: application/json" \
  -d @ism_policy.json \
  "https://<OPENSEARCH_ENDPOINT>/_plugins/_ism/policies/etl-logs-retention"
```

Expected response:

```json
{
  "_id": "etl-logs-retention",
  "_version": 1,
  "_seq_no": 0,
  "_primary_term": 1,
  "policy": { ... }
}
```

### 3. Verify the policy is attached to the index

After the index is created and the ISM policy exists, OpenSearch automatically attaches the policy to any index matching `etl-logs-*`. Verify with:

```bash
awscurl --service es --region <AWS_REGION> \
  -X GET \
  "https://<OPENSEARCH_ENDPOINT>/_plugins/_ism/explain/etl-logs-<YYYY-MM>"
```

### 4. Update an existing policy

If you need to update the retention period or add states, increment the `seq_no` and `primary_term` from the GET response:

```bash
awscurl --service es --region <AWS_REGION> \
  -X PUT \
  -H "Content-Type: application/json" \
  -d @ism_policy.json \
  "https://<OPENSEARCH_ENDPOINT>/_plugins/_ism/policies/etl-logs-retention?if_seq_no=<SEQ_NO>&if_primary_term=<PRIMARY_TERM>"
```

---

## Notes

- The `awscurl` tool handles SigV4 request signing automatically. Install it with `pip install awscurl`.
- If your OpenSearch domain uses fine-grained access control, ensure the IAM role used to run these commands has the `es:ESHttpPut` permission on the domain ARN.
- For VPC-deployed domains, run these commands from an EC2 instance or Cloud9 environment inside the same VPC, or use an AWS Systems Manager Session Manager tunnel.
