from airflow.decorators import dag
from datetime import datetime
from tasks.etl_tasks import execute_etl

@dag(
    dag_id="etl_dag",
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 2), 
    catchup=False,
    tags=["ETL"]
)
def etl_process():
    execute_etl()

dag_instance = etl_process()