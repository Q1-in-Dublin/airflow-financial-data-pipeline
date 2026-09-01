# Quality Monitor Agent — Feature Specification

**Project**: airflow-financial-data-pipeline (Phase 2)
**Branch**: `feature/quality-agent`
**Status**: Draft

---

## 1. Objective

Track quality metrics (missing-value rate, negative-amount rate, row counts, etc.) for every run of the `financial_pipeline` DAG over time, and automatically detect anomalies that deviate from normal patterns. When an anomaly is detected, an LLM generates a natural-language report from the structured metric diff, and RAG retrieves similar past incidents to include as context.

The goal is to let a data engineer understand pipeline health and likely root causes at a glance, without manually digging through Airflow logs.

## 2. Use Cases

| User                           | Scenario                                                                                                            |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| Data engineer (me)             | Checks the generated report after each pipeline run; quickly investigates if there's an anomaly, otherwise moves on |
| (Hypothetical) New team member | Uses past incident reports to get up to speed quickly when encountering a pipeline issue for the first time         |

## 3. Key Functionality

1. Record quality metrics into a `pipeline_run_metrics` table on every `financial_pipeline` run
2. Anomaly detection logic that compares today's metrics against the rolling N-day (default 7-day) mean/standard deviation and flags threshold breaches
3. When an anomaly is detected, pass the structured metric diff to an LLM (Claude API) to generate a natural-language report
4. Embed past reports into a vector store (Chroma); when a new anomaly occurs, retrieve similar past cases via RAG and include them in the report
5. Save the report as a markdown file in the `reports/` directory

## 4. Architecture

```
financial_pipeline DAG (existing)
      │
      ├─ load task completes → INSERT into pipeline_run_metrics
      │
      ▼ (connected via Dataset or ExternalTaskSensor)
quality_monitor DAG (new)
      │
      ├─ fetch_recent_metrics        : query metrics from the last N days
      ├─ detect_anomaly              : threshold-based anomaly detection (plain pandas)
      ├─ [only runs if an anomaly is detected]
      ├─ retrieve_similar_incidents  : retrieve similar past reports from Chroma (RAG)
      ├─ generate_report             : call Claude API to generate the natural-language report
      └─ save_report                 : save reports/YYYY-MM-DD.md + add embedding to Chroma
```

**Why a separate DAG**: ETL execution (`financial_pipeline`) and monitoring/analysis (`quality_monitor`) have different responsibilities. Isolating them ensures failures in external dependencies (LLM API, vector DB) don't affect core data loading.

**DAG linkage**: Prefer Airflow 3.x Dataset-based scheduling (the `load` task declares an update to `transactions` as a Dataset, and `quality_monitor` subscribes to it). Fall back to `ExternalTaskSensor` if Dataset scheduling isn't practical.

## 5. Data Model

### `pipeline_run_metrics` (new table)

| Column                    | Type      | Description                       |
| ------------------------- | --------- | --------------------------------- |
| run_id                    | text (PK) | Airflow run_id                    |
| dag_id                    | text      | DAG that produced this run        |
| execution_date            | timestamp | Execution timestamp               |
| file_name                 | text      | Source file processed             |
| total_rows                | int       | Total rows processed              |
| missing_amount_count      | int       | Rows missing `amount`             |
| missing_customer_id_count | int       | Rows missing `customer_id`        |
| negative_amount_count     | int       | Rows with negative amount         |
| rejection_rate            | float     | (missing + negative) / total_rows |
| created_at                | timestamp | When this row was recorded        |

### `reports/` (file-based)

- Filename: `reports/{execution_date}.md`
- Content: anomaly summary, related metrics, links to similar past incidents

### Vector store (Chroma, local)

- Collection: `incident_reports`
- Documents: full text of previously generated reports
- Metadata: execution_date, anomaly_type

## 6. Technical Requirements

- **Database**: Add the `pipeline_run_metrics` table to the existing `financial_pipeline` PostgreSQL database (no new DB needed)
- **Anomaly detection**: Plain Python/pandas, no external API calls — fully local for a fast feedback loop
- **LLM integration**: Anthropic API. **Called only when an anomaly is detected** (cost control — not invoked on every run)
- **RAG**: Local, persisted Chroma vector store. Start with a local `sentence-transformers` embedding model (avoids embedding API cost); keep the embedding function in a separate module so it can be swapped for an Anthropic/OpenAI embedding later
- **DAG linkage**: Prefer Airflow 3.x Dataset scheduling; fall back to `ExternalTaskSensor`
- **Failure isolation**: A `quality_monitor` DAG failure must not affect `financial_pipeline`'s retries or alerting — fully independent DAG

## 7. Testing (Shift Left)

| Level       | Target                        | Method                                                                                                  |
| ----------- | ----------------------------- | ------------------------------------------------------------------------------------------------------- |
| Unit        | `detect_anomaly` logic        | Verify with normal/anomalous fixture data (can reuse the existing `transactions_dirty.csv` run history) |
| Unit        | Metric calculation functions  | Known input → known `rejection_rate` output                                                             |
| Integration | `pipeline_run_metrics` INSERT | Insert into a test DB and verify via query                                                              |
| Integration | RAG retrieval                 | Seed a few dummy incidents and verify similarity search results                                         |
| Mock        | LLM call                      | Verify `generate_report` logic with mocked responses, no real API call (cost/speed)                     |

**Self-verification criteria (completion conditions to hand the agent)**:

- `detect_anomaly` unit tests pass with ≥90% coverage
- Includes a migration script for the `pipeline_run_metrics` schema
- The LLM call path is covered by mock tests; the full test suite must pass without a real API key

## 8. Out of Scope (for this phase)

- Real-time Slack/email alerting integration (local markdown output only, for now)
- Production deployment / scheduling optimization
- Extending to multiple pipelines (currently scoped to `financial_pipeline` only)

## 9. Rollout Order

1. Create `pipeline_run_metrics` table + add recording logic to `financial_pipeline`
2. `quality_monitor` DAG skeleton + Dataset/Sensor linkage
3. `detect_anomaly` pure logic + unit tests
4. `generate_report` (LLM integration, with mock tests)
5. RAG layer (Chroma setup + `retrieve_similar_incidents`)
6. Update README (add Phase 2 section)

---

_This document is intended to be handed to an agent as context when delegating implementation work._
