from airflow.decorators import dag
from datetime import datetime
from tasks.ingestion_task import (
    m_ingest_data_into_suppliers,
    m_ingest_data_into_products,
    m_ingest_data_into_customers,
    m_ingest_data_into_sales
)
from tasks.m_supplier_perfomance_task import m_load_suppliers_perfomance
from tasks.m_product_performance_task import  m_load_products_perfomance

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
    supplier_perfomance = m_load_suppliers_perfomance()
    product_perfomance = m_load_products_perfomance()
    
    [supplier_task, product_task, customer_task, sale_task] >> supplier_perfomance >> product_perfomance

dag_instance = etl_process()
