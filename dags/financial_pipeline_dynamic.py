from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import os

DATA_DIR = "/opt/airflow/data"
POSTGRES_CONN_ID = "financial_pipeline_db"

@dag(
    dag_id="financial_pipeline_dynamic", 
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["portfolio", "financial-etl", "dynamic-mapping"],
)
def financial_pipeline_dynamic():

    @task
    def list_files():
        all_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]

        files = [
            f for f in all_files
            if not f.startswith(("clean_", "rejected_", "cleantransactions"))
    ]
        print(f"File Found: {files}")
        return files

    @task
    def extract(filename: str):
        path = os.path.join(DATA_DIR, filename)
        df = pd.read_csv(path)
        print(f"{filename}: {len(df)}rows loaded")
        return filename


    @task
    def validate(filename: str, **context):
        path = os.path.join(DATA_DIR, filename)
        df = pd.read_csv(path)

        expected_columns = [
            "transaction_id", "trade_date", "customer_id", "account_id",
            "security_id", "amount", "currency", "country",
        ]

        report = {
            "filename": filename,
            "total_rows": len(df),
            "schema_ok": list(df.columns) == expected_columns,
            "missing_amount": int(df["amount"].isna().sum()),
            "missing_customer_id": int(df["customer_id"].isna().sum()),
            "negative_amount": int((df["amount"] < 0).sum()),
        }

        print(f"Validation Report: {report}")

        ti = context["ti"]
        ti.xcom_push(key="validation_report", value=report)

        return filename


    @task
    def transform(filename:str):
        path = os.path.join(DATA_DIR, filename)
        df = pd.read_csv(path)

        before = len(df)
        is_bad = df["amount"].isna() | df["customer_id"].isna() | (df["amount"] < 0)

        good_df = df[~is_bad].copy()
        bad_df = df[is_bad].copy()
        bad_df["rejection_reason"] = "missing_or_negative_amount"

        print(f"Before Filtered: {before}rows → Normal: {len(good_df)}rows, rejexted: {len(bad_df)}row")

        clean_filename = f"clean_{filename}"
        rejected_filename = f"rejected_{filename}"

        good_df.to_csv(os.path.join(DATA_DIR, clean_filename), index=False)
        bad_df.to_csv(os.path.join(DATA_DIR, rejected_filename), index=False)

        return {"clean": clean_filename, "rejected": rejected_filename}

    @task
    def load_all(file_info: list):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        total_clean = 0
        total_rejected = 0
        for info in file_info:
            clean_path = os.path.join(DATA_DIR, info["clean"])
            clean_df = pd.read_csv(clean_path)
            
            if len(clean_df) > 0:
                clean_df.to_sql("transactions", engine, if_exists="append", index=False)
                total_clean += len(clean_df)

            rejected_path = os.path.join(DATA_DIR, info["rejected"])
            rejected_df = pd.read_csv(rejected_path)
            if len(rejected_df) > 0:
                rejected_df.to_sql("rejected_transactions", engine, if_exists="append", index=False)
                total_rejected += len(rejected_df)

    files = list_files()
    extracted = extract.expand(filename=files)
    validated = validate.expand(filename=extracted)
    transformed = transform.expand(filename=validated)
    load_all(transformed)


financial_pipeline_dynamic() 