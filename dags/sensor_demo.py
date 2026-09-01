from airflow.sdk import dag, task
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime


DATA_DIR = "/opt/airflow/data"

@dag(
    dag_id="sensor_demo",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["day7-8", "sensors"],
)

def sensor_demo():
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="incoming_signal.csv",   # DATA_DIR based path
        fs_conn_id="fs_default",           # file path Connection
        poke_interval=10,                  # check every 10 secodns
        timeout=120,                       # fail process not appear in 120 sec
        mode="poke",
    )

    @task
    def process_file():
        path = f"{DATA_DIR}/incoming_signal.csv"
        print(f"file found start processing: {path}")

    wait_for_file >> process_file()

sensor_demo()