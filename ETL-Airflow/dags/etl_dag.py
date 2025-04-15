
from airflow.decorators import dag
from datetime import datetime
from apps.population_etl_task import load_usa_population_to_pg

@dag(dag_id ="etl_taskflow",schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False, tags=['etl', 'spark'])
def etl_task():
    load_usa_population_to_pg()

dag_instance = etl_task()