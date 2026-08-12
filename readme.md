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
   Validate  → check schema, missing values, outliers
      ↓
   Transform → clean data (drop nulls, remove negative amounts)
      ↓
   Load      → write to PostgreSQL via PostgresHook
```

## Tech Stack

- **Apache Airflow 3.3.0** (Docker Compose, official quick-start setup)
- **PostgreSQL 16** (separate `financial_pipeline` database, isolated from Airflow's own metadata DB)
- **Python** — pandas, Faker, SQLAlchemy
- **TaskFlow API** (`@dag`, `@task` decorators)

## DAG: `financial_pipeline`

| Task               | Description                                                                                                            |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| `list_files`       | Scans `data/` directory for CSV files                                                                                  |
| `pick_target_file` | Selects the target file to process                                                                                     |
| `extract`          | Loads CSV into a pandas DataFrame, logs row count                                                                      |
| `validate`         | Checks schema conformity, missing values (`amount`, `customer_id`), and negative amounts; produces a validation report |
| `transform`        | Drops rows with missing critical fields and negative amounts; writes a cleaned CSV                                     |
| `load`             | Loads the cleaned data into a PostgreSQL table via `PostgresHook`                                                      |

Task dependencies are expressed via TaskFlow's implicit XCom chaining (function calls passing return values downstream), equivalent to `list_files >> pick_target_file >> extract >> validate >> transform >> load`.

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

Running the pipeline against `transactions_dirty.csv` (200 rows, with injected issues) successfully:

- Detected missing `amount`/`customer_id` values and negative amounts during validation
- Removed invalid rows during transform
- Loaded **185 clean rows** into the `transactions` table in PostgreSQL

```sql
SELECT COUNT(*) FROM transactions;
-- 185
```

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
│   └── financial_pipeline.py
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

- **Dynamic Task Mapping** — process all CSV files (not just one hardcoded target) without defining N separate tasks
- **Sensors** — wait on file arrival rather than assuming files are already present
- **Backfill / catchup** — reprocess a specific past data interval on demand
- Unit tests for `transform`/`validate` logic
- Optional: swap the CSV extract step for a public API call (e.g. FX rates) to demonstrate API integration

## Learning Context

This project was built over a condensed 8-day self-study plan covering:

1. Airflow architecture fundamentals (DAG, Task, DAG Run, Scheduler, Executor, Worker)
2. Writing DAGs with sequential, parallel, and branching task dependencies
3. Scheduling concepts (data intervals, `catchup`, retries)
4. **This pipeline** — a full extract → validate → transform → load implementation
5. _(planned)_ Dynamic Task Mapping, Sensors, and backfill/catchup

Along the way, this also involved troubleshooting real-world issues: Docker volume mounts, `venv`/`PATH` conflicts, Git repository scope mistakes, GitHub credential/authentication issues, and connection configuration errors (e.g. a stray whitespace character in a hostname causing a DNS resolution failure) — all fixed through log-driven debugging.
