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

        print(f"검증 리포트: {report}")

        # 명시적 XCom push: report 전체를 별도 key로 저장
        ti = context["ti"]
        ti.xcom_push(key="validation_report", value=report)

        return filename


    @task
    def transform(filename:str):
        path = os.path.join(DATA_DIR, filename)
        df = pd.read_csv(path)

        before = len(df)
        # remove rows customer_id is null
        #df = df.dropna(subset=["amount", "customer_id"])
        is_bad = df["amount"].isna() | df["customer_id"].isna() | (df["amount"] < 0)

        good_df = df[~is_bad].copy()
        bad_df = df[is_bad].copy()
        bad_df["rejection_reason"] = "missing_or_negative_amount"
        # remove weird amount (only positive)
        # df = df[df["amount"] >= 0]

        print(f"Before Filtered: {before}rows → Normal: {len(good_df)}rows, rejexted: {len(bad_df)}row")

        clean_filename = f"clean_{filename}"
        rejected_filename = f"rejected_{filename}"

        good_df.to_csv(os.path.join(DATA_DIR, clean_filename), index=False)
        bad_df.to_csv(os.path.join(DATA_DIR, rejected_filename), index=False)

        return {"clean": clean_filename, "rejected": rejected_filename}
        # cleaned_filename = f"clean{filename}"
        # clean_path = os.path.join(DATA_DIR, cleaned_filename)
        # df.to_csv(clean_path, index=False)

        # return cleaned_filename

    @task
    def load(file_info: dict):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        # 정상 데이터: append (매번 지우지 않고 쌓기)
        clean_path = os.path.join(DATA_DIR, file_info["clean"])
        clean_df = pd.read_csv(clean_path)
        clean_df.to_sql("transactions", engine, if_exists="append", index=False)
        print(f"transactions 테이블에 {len(clean_df)}건 적재 (append)")

        # 반려 데이터: 별도 테이블에 append
        rejected_path = os.path.join(DATA_DIR, file_info["rejected"])
        rejected_df = pd.read_csv(rejected_path)
        if len(rejected_df) > 0:
            rejected_df.to_sql("rejected_transactions", engine, if_exists="append", index=False)
            print(f"rejected_transactions 테이블에 {len(rejected_df)}건 적재")

    files = list_files()
    target_file = pick_target_file(files)
    extracted = extract(target_file)
    validated = validate(extracted)
    cleaned = transform(validated)
    load(cleaned)


financial_pipeline()