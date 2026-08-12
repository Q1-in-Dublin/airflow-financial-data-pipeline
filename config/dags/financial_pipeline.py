from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import os

DATA_DIR = "/opt/airflow/data"
POSTGRES_CONN_ID = "financial_pipeline_db"

@dag(
    dag_id="financial_pipeline",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["portfolio", "financial-etl"],
)
def financial_pipeline():

    @task
    def list_files():
        files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
        print(f"File Found: {files}")
        return files

    # @task
    # def pick_first_file(files: list):
    #     first = sorted(files)[0]   # 정렬해서 첫 번째 (순서 일관성 위해)
    #     print(f"Selected File: {first}")
    #     return first
    @task
    def pick_target_file(files: list):
        target = "transactions_dirty.csv"
        assert target in files, f"{target} No exists: {files}"
        print(f"Selected File: {target}")
        return target

    # @task
    # def extract(filename: str):
    #     path = os.path.join(DATA_DIR, filename)
    #     df = pd.read_csv(path)
    #     print(f"{filename}: {len(df)} loaded")
    #     return {
    #         "filename": filename,
    #         "row_count": len(df),
    #         "columns": list(df.columns),
    #     }
    @task
    def extract(filename: str):
        path = os.path.join(DATA_DIR, filename)
        df = pd.read_csv(path)
        print(f"{filename}: {len(df)}건 loaded")
        return filename


    @task
    def vallidation(filename:str):
        path = os.path.join(DATA_DIR,filename)
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
        return filename  # Pass file name for tranformation

    @task
    def tranform(filename:str):
        path = os.path.join(DATA_DIR, filename)
        df = pd.read_csv(path)

        before = len(df)
        # remove rows customer_id is null
        df = df.dropna(subset=["amount", "customer_id"])

        # remove weird amount (only positive)
        df = df[df["amount"] >= 0]

        after = len(df)
        print(f"Before transdformation: {before} cases → After transformation: {after}cases (removed: {before - after}cases)")

        cleaned_filename = f"clean{filename}"
        clean_path = os.path.join(DATA_DIR, cleaned_filename)
        df.to_csv(clean_path, index=False)

        return cleaned_filename

    @task

    def load(cleaned_filename:str):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        path = os.path.join(DATA_DIR,cleaned_filename)
        df = pd.read_csv(path)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        df.to_sql(
            "transactions",
            engine,
            if_exists="replace",  
            index=False,
        )
        print(f"{len(df)} rows are loaded in the transactions table ")

    files = list_files()
    target_file = pick_target_file(files)
    extract = extract(target_file)
    vallidation = vallidation(extract)
    cleaned = tranform(vallidation)
    load(cleaned)

financial_pipeline()