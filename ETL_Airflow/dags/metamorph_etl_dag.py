from airflow.decorators import dag
from datetime import datetime
from tasks.ingestion_data import (
    m_ingest_data_into_suppliers,
    m_ingest_data_into_products,
    m_ingest_data_into_customers,
    m_ingest_data_into_sales,
    m_calculate_supplier_performance
)

@dag(
    dag_id="metamorph_etl_pipeline",
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 6),
    catchup=False,
    tags=["ETL"]
)
def etl_process():
    supplier_task = m_ingest_data_into_suppliers()
    product_task = m_ingest_data_into_products()
    customer_task = m_ingest_data_into_customers()
    sale_task = m_ingest_data_into_sales()
    supplier_perfomance=m_calculate_supplier_performance()

    [supplier_task , product_task ,customer_task ,sale_task] >> supplier_perfomance


dag_instance = etl_process()
