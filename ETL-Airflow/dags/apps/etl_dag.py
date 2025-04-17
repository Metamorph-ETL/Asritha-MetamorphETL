from airflow.decorators import dag
from datetime import datetime
from tasks.etl_tasks import  m_load_data

@dag(dag_id ="population_etl",schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False, tags=['etl', 'spark'])
def etl_task():
    m_load_data()

dag_instance = etl_task()