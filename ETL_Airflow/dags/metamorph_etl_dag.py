from airflow.decorators import dag
from datetime import datetime
from tasks.ingestion_data import (
    m_ingest_data_into_suppliers,
    m_ingest_data_into_products,
    m_ingest_data_into_customers
)

@dag(
    dag_id="metamorph_etl_pipeline",
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 6),
    catchup=False,
    tags=["ETL"]
)
def etl_process():
   m_ingest_data_into_suppliers()
   m_ingest_data_into_products()
   m_ingest_data_into_customers()


dag_instance = etl_process()