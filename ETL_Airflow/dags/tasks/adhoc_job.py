from tasks.adhoc.adhoc_column_order import m_adhoc_into_customers, m_adhoc_into_suppliers,m_adhoc_into_products,m_adhoc_into_sales
from airflow.decorators import dag
@dag(
    dag_id="adhoc_customers",
    tags=["ETL"]
)

def etl_process():
    m_adhoc_into_customers()
    m_adhoc_into_suppliers()
    m_adhoc_into_sales()
    m_adhoc_into_products()


dag_instance = etl_process()

