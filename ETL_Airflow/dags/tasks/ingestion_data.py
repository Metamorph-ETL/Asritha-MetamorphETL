import logging
from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import create_session, transform_data, load_to_postgres, Extractor, log
from secret_key import SUPPLIERS_URL, PRODUCTS_URL, CUSTOMERS_URL
from airflow.hooks.base import BaseHook
@task
def m_ingest_data_into_suppliers():
    try:
        log.info("Starting ETL process for suppliers")
        spark = create_session()
        log.info("Spark session initialized")

        extractor = Extractor(SUPPLIERS_URL)
        data = extractor.extract_data()
        log.info(f"Extracted {len(data)} records from suppliers")

        SQ_Shortcut_To_Supplier = transform_data(spark, data, ["SUPPLIER_ID"])
        Shortcut_To_Supplier_tgt = SQ_Shortcut_To_Supplier

        load_to_postgres(Shortcut_To_Supplier_tgt, "raw.supplier")
        log.info("Loaded suppliers data into PostgreSQL successfully")

    except Exception:
        log.error("Suppliers ETL failed", exc_info=True)
        raise

    finally:
        spark.stop()
        log.info("Spark session stopped")

@task
def m_ingest_data_into_products():
    try:
        log.info("Starting ETL process for products")
        spark = create_session()
        log.info("Spark session initialized")

        extractor = Extractor(PRODUCTS_URL)
        data = extractor.extract_data()
        log.info(f"Extracted {len(data)} records from products")

        SQ_Shortcut_To_Product = transform_data(spark, data, ["PRODUCT_ID"])
        Shortcut_To_Product_tgt = SQ_Shortcut_To_Product

        load_to_postgres(Shortcut_To_Product_tgt, "raw.product")
        log.info("Loaded products data into PostgreSQL successfully")

    except Exception:
        log.error("Products ETL failed", exc_info=True)
        raise

    finally:
        spark.stop()
        log.info("Spark session stopped")

@task
def m_ingest_data_into_customers():
    try:
        log.info("Starting ETL process for customers")
        spark = create_session()

        conn = BaseHook.get_connection("customers_api")
        extractor = Extractor(CUSTOMERS_URL, token=conn.password)

        data = extractor.extract_data()
        log.info(f"Extracted {len(data)} customer records")

        SQ_Shortcut_To_Customer = transform_data(spark, data, ["CUSTOMER_ID"])
        Shortcut_To_Customer_tgt = SQ_Shortcut_To_Customer

        load_to_postgres(Shortcut_To_Customer_tgt, "raw.customer")
        log.info("Loaded customers successfully")

    except Exception as e:
        log.error(f"Customers ETL failed: {str(e)}", exc_info=True)
        raise

    finally:
        spark.stop()
        log.info("Spark session stopped")
