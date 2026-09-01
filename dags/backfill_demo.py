from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="backfill_demo",
    start_date=datetime(2026, 8, 25),   # Setting the backfill date
    schedule="@daily",                   # Working everyday
    catchup=False,                       # only the past
    tags=["day7-8", "backfill"],
)
def backfill_demo():

    @task
    def process_daily_data(**context):
        data_interval_start = context["data_interval_start"]
        print(f"subject dates: {data_interval_start.date()}")
        print(f"(Processes is running but it works for the date abovre)")

    process_daily_data()

backfill_demo()