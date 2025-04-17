from airflow.decorators import dag
from datetime import datetime
# Import the ETL task from the etl_tasks folder. Ensure your PYTHONPATH is set appropriately.
from tasks.etl_tasks import load_usa_population_to_pg

@dag(dag_id ="etl_taskflow",schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False, tags=['etl', 'spark'])
def etl_task():
    load_usa_population_to_pg()

dag_instance = etl_task()