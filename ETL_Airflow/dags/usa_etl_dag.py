from airflow.decorators import dag
from datetime import datetime
# Import the ETL task from the etl_tasks folder. Ensure your PYTHONPATH is set appropriately.
from tasks.usa_population_ingestion import m_load_usa

@dag(dag_id ="etl_taskflow",schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False, tags=['etl', 'spark'])
def etl_task():
    m_load_usa()

dag_instance = etl_task() 