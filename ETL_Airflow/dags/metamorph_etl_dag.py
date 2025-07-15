from airflow.decorators import dag
from datetime import datetime,timedelta
from tasks.ingestion_task import (
    m_ingest_data_into_suppliers,
    m_ingest_data_into_products,
    m_ingest_data_into_customers,
    m_ingest_data_into_sales
)
from tasks.m_supplier_performance_task import m_load_suppliers_performance
from tasks.m_product_performance_task import  m_load_products_performance
from tasks.m_customer_sales_report_task import m_load_customer_sales_report

@dag(
    dag_id="metamorph_etl_pipeline",
    catchup=False,
    tags=["ETL"]
)

def etl_process():
    supplier_task = m_ingest_data_into_suppliers()
    product_task = m_ingest_data_into_products()
    customer_task = m_ingest_data_into_customers()
    sale_task = m_ingest_data_into_sales()
    supplier_performance = m_load_suppliers_performance()
    product_performance = m_load_products_performance()
    customer_sales_report = m_load_customer_sales_report()
    
    [supplier_task, product_task, customer_task, sale_task] >> supplier_performance >> product_performance >> customer_sales_report

dag_instance = etl_process()


