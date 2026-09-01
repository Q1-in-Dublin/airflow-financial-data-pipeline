Readme · MD

# Airflow Financial Data Pipeline

A local, end-to-end ETL pipeline built with **Apache Airflow 3.x** and **PostgreSQL**, using synthetic financial transaction data. This project was built as a hands-on portfolio piece to demonstrate practical Airflow orchestration skills for a Data Engineer role transition.

## Project Purpose

Rather than only completing tutorials, this project validates, transforms, and loads realistic (Faker-generated) financial transaction data through a working Airflow DAG — including deliberately injected data quality issues (missing values, negative amounts) to exercise real validation logic.

## Architecture

```
CSV files (Faker-generated)
      ↓
   Airflow DAG (orchestration)
      ↓
   Extract   → read CSV with pandas
      ↓
   Validate  → check schema, missing values, outliers (pushes report via explicit XCom)
      ↓
   Transform → split into clean vs. rejected records
      ↓
   Load      → append clean rows to `transactions`, rejected rows to `rejected_transactions`
```

## Tech Stack

- **Apache Airflow 3.3.0** (Docker Compose, official quick-start setup)
- **PostgreSQL 16** (separate `financial_pipeline` database, isolated from Airflow's own metadata DB)
- **Python** — pandas, Faker, SQLAlchemy
- **TaskFlow API** (`@dag`, `@task` decorators)

## Additional DAGs (Day 7-8)

Beyond the main `financial_pipeline`, this repo includes three focused DAGs that each demonstrate one Airflow concept in isolation, since these are common interview topics and easier to reason about separately from the full pipeline:

| DAG                          | Concept                  | What it demonstrates                                                                                                                                                                                                                                                                                                                              |
| ---------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `financial_pipeline_dynamic` | **Dynamic Task Mapping** | `extract`, `validate`, and `transform` use `.expand()` to automatically fan out over however many CSV files exist in `data/`, with no DAG code changes needed as file count grows. `load_all` then reduces the mapped results back into a single load step.                                                                                       |
| `sensor_demo`                | **Sensors**              | A `FileSensor` polls for a file's arrival (`poke_interval=10s`) rather than assuming the file already exists, mirroring how a real pipeline would wait on an upstream system.                                                                                                                                                                     |
| `backfill_demo`              | **Backfill / Catchup**   | Demonstrates that a DAG Run's `data_interval_start` — not its actual execution time — determines which date it processes. Running `airflow backfill create --from-date ... --to-date ...` regenerates historical runs on demand, confirmed by task logs showing the target date (e.g. `2026-08-25`) even though the run executed on `2026-09-01`. |

## DAG: `financial_pipeline`

| Task               | Description                                                                                                                                                                                                 |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `list_files`       | Scans `data/` directory for CSV files                                                                                                                                                                       |
| `pick_target_file` | Selects the target file to process                                                                                                                                                                          |
| `extract`          | Loads CSV into a pandas DataFrame, logs row count                                                                                                                                                           |
| `validate`         | Checks schema conformity, missing values (`amount`, `customer_id`), and negative amounts; pushes the validation report to a dedicated XCom key (`validation_report`), separate from the task's return value |
| `transform`        | Splits rows into clean and rejected sets (missing critical fields or negative amounts), tagging rejected rows with a `rejection_reason`, and writes both to separate CSVs                                   |
| `load`             | Loads clean rows into `transactions` and rejected rows into `rejected_transactions`, both via `PostgresHook`, using append-only writes so each run adds to history rather than overwriting it               |

Task dependencies are expressed via TaskFlow's implicit XCom chaining (function calls passing return values downstream), equivalent to `list_files >> pick_target_file >> extract >> validate >> transform >> load`.

`validate` also demonstrates **explicit XCom usage** (`ti.xcom_push(key=..., value=...)`) alongside TaskFlow's automatic return-value XCom, to push a structured report under its own key rather than overloading the task's return value.

## Data

Synthetic transaction data generated with the `Faker` library (`src/generate_data.py`), with the following schema:

| Column           | Description                   |
| ---------------- | ----------------------------- |
| `transaction_id` | Unique transaction ID         |
| `trade_date`     | Trade date                    |
| `customer_id`    | Customer ID                   |
| `account_id`     | Account ID                    |
| `security_id`    | Fake ISIN-style security code |
| `amount`         | Transaction amount            |
| `currency`       | Currency (EUR/USD/GBP)        |
| `country`        | Country code                  |

Four files are generated:

- `transactions_001.csv` / `002.csv` / `003.csv` — clean data (500 rows each)
- `transactions_dirty.csv` — 200 rows with intentionally injected missing values and negative amounts, used to exercise the `validate` and `transform` logic
  **Note:** No real company data, schemas, or field names are used anywhere in this project. All data is synthetic and generated locally.

## Result

Running the pipeline against `transactions_dirty.csv` (200 rows, with injected issues):

- Validation detected 12 rows with missing `amount` and 3 rows with negative `amount` (15 rows total flagged)
- Transform split the data into clean and rejected sets, tagging each rejected row with a `rejection_reason`
- Load appended the clean rows to `transactions` and the rejected rows to `rejected_transactions`, so re-running the DAG accumulates history in both tables rather than overwriting it
  `transactions` reflects two accumulated runs against `transactions_dirty.csv` (185 clean rows each); `rejected_transactions` was introduced in this iteration, so it currently reflects one run:

```sql
SELECT COUNT(*) FROM transactions;
-- 370  (185 clean rows × 2 runs, confirming append-only behavior)

SELECT COUNT(*) FROM rejected_transactions;
-- 15   (12 missing amount + 3 negative amount, from the latest run)
```

**Dynamic Task Mapping** (`financial_pipeline_dynamic`) was verified against all four source files at once, after resetting both tables:

```sql
SELECT COUNT(*) FROM transactions;
-- 1685  (500 + 500 + 500 + 185 clean rows across all four files)

SELECT COUNT(*) FROM rejected_transactions;
-- 15    (all from transactions_dirty.csv, the only file with injected issues)
```

This confirms `extract`/`validate`/`transform` correctly fanned out over all four files without any hardcoded file selection.

## Setup

```bash
# 1. Create virtual environment and install dependencies
python3 -m venv venv
source venv/bin/activate
pip install faker pandas

# 2. Generate synthetic data
./venv/bin/python src/generate_data.py

# 3. Start Airflow (Docker Compose)
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up -d

# 4. Create a dedicated Postgres database for pipeline data
docker compose exec postgres psql -U airflow -c "CREATE DATABASE financial_pipeline;"

# 5. Register a Postgres connection in Airflow UI
# Admin > Connections > Add:
#   Connection Id: financial_pipeline_db
#   Connection Type: Postgres
#   Host: postgres
#   Login: airflow
#   Password: airflow
#   Port: 5432
#   Database: financial_pipeline
```

Airflow UI: [http://localhost:8080](http://localhost:8080) (login: `airflow` / `airflow`)

## Repository Structure

```
airflow-financial-data-pipeline/
├── dags/
│   ├── financial_pipeline.py
│   ├── financial_pipeline_dynamic.py   # Dynamic Task Mapping
│   ├── sensor_demo.py                  # Sensors
│   └── backfill_demo.py                # Backfill / Catchup
├── src/
│   └── generate_data.py
├── data/
│   ├── transactions_001.csv
│   ├── transactions_002.csv
│   ├── transactions_003.csv
│   └── transactions_dirty.csv
├── config/
├── plugins/
├── tests/            # planned
├── docker-compose.yaml
├── .gitignore
└── README.md
```

## What's Next

- **Upsert logic** — key rows by `transaction_id` so reprocessing the same source data doesn't create duplicates (the current `financial_pipeline` and `financial_pipeline_dynamic` DAGs are append-only, so re-running them accumulates history rather than deduplicating)
- **Merge `financial_pipeline_dynamic`'s mapping approach into the main pipeline**, replacing the single hardcoded target file
- Unit tests for `transform`/`validate` logic
- Optional: swap the CSV extract step for a public API call (e.g. FX rates) to demonstrate API integration
- Optional: package the local Docker Compose setup more formally for one-command reproducibility

## Learning Context

This project was built over a condensed 8-day self-study plan covering:

1. Airflow architecture fundamentals (DAG, Task, DAG Run, Scheduler, Executor, Worker)
2. Writing DAGs with sequential, parallel, and branching task dependencies
3. Scheduling concepts (data intervals, `catchup`, retries)
4. A full extract → validate → transform → load implementation
5. Explicit XCom usage and splitting output into clean vs. rejected data stores
6. Dynamic Task Mapping, Sensors, and Backfill/Catchup — verified with `financial_pipeline_dynamic`, `sensor_demo`, and `backfill_demo`
   Along the way, this also involved troubleshooting real-world issues: Docker volume mounts, `venv`/`PATH` conflicts, Git repository scope mistakes, GitHub credential/authentication issues, connection configuration errors (e.g. a stray whitespace character in a hostname causing a DNS resolution failure), an accidentally committed secret (`fernet_key`) that required rewriting Git history, DAG files landing in the wrong directory or under a mismatched `dag_id`, and CLI argument names that shifted between Airflow versions — all fixed through log-driven debugging.

## Roadmap: Quality Monitoring Agent (In Progress)

Phase 2 adds an anomaly detection + LLM-based reporting agent on top of this pipeline.

Design doc: [docs/quality-monitor-spec.md](docs/quality-monitor-spec.md)
Working branch: `feature/quality-agent`
