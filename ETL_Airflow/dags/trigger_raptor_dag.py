from airflow.decorators import dag
from datetime import datetime
from tasks.raptor_task import raptor_call

@dag(
    dag_id="raptor_testing",
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 6),
    catchup=False, 
    tags=["raptor"]
)

def testing():
     raptor_call()

raptor = testing()