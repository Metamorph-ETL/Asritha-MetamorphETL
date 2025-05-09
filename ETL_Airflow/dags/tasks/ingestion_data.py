import logging
from pyspark.sql.functions import count
from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import create_session, load_to_postgres, Extractor, log,Duplicate_check
from secret_key import SUPPLIERS_URL, PRODUCTS_URL, CUSTOMERS_URL,TOKEN_URL

#create a task that ingests data into raw.suppliers table
@task
def m_ingest_data_into_suppliers():
    try:
        spark = create_session()
        
        # Extract supplier data from API
        extractor = Extractor(SUPPLIERS_URL)
        data = extractor.extract_data()
        
         # Convert extracted JSON data to Spark DataFrame
        suppliers_df = spark.createDataFrame(data)

        # Rename columns
        SQ_Shortcut_To_Supplier = (
            suppliers_df
            .withColumnRenamed("supplier_id", "SUPPLIER_ID")
            .withColumnRenamed("supplier_name", "SUPPLIER_NAME")
            .withColumnRenamed("contact_details", "CONTACT_DETAILS")
            .withColumnRenamed("region", "REGION")
        ) .select("SUPPLIER_ID", "SUPPLIER_NAME", "CONTACT_DETAILS", "REGION")
        
        # Check for duplicate SUPPLIER_IDs
        Duplicate_check.has_duplicates(SQ_Shortcut_To_Supplier, ["SUPPLIER_ID"])
        
        logging.info("Data is transformed")
        
         # Load the cleaned data into the raw.suppliers table
        load_to_postgres(SQ_Shortcut_To_Supplier, "raw.suppliers")
     
    except Exception as e:
        log.error(f"Suppliers ETL failed: {str(e)}", exc_info=True)
        raise

    finally:
        spark.stop()
        log.info("Spark session stopped")

#create a task that ingests data into raw.products table
@task
def m_ingest_data_into_products():
    try:
        spark = create_session()
        
        # Extract products data from API
        extractor = Extractor(PRODUCTS_URL)
        data = extractor.extract_data()
         
        # Convert extracted JSON data to Spark DataFrame
        products_df = spark.createDataFrame(data)

         # Rename columns
        SQ_Shortcut_To_Product = (
            products_df
            .withColumnRenamed("product_id", "PRODUCT_ID")
            .withColumnRenamed("product_name", "PRODUCT_NAME")
            .withColumnRenamed("category", "CATEGORY")
            .withColumnRenamed("price", "PRICE")
            .withColumnRenamed("stock_quantity", "STOCK_QUANTITY")
            .withColumnRenamed("reorder_level", "REORDER_LEVEL")
            .withColumnRenamed("supplier_id", "SUPPLIER_ID")
            ).select("PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "PRICE", "STOCK_QUANTITY", "REORDER_LEVEL", "SUPPLIER_ID")
        
        # Check for duplicate PRODUCT_IDs
        Duplicate_check.has_duplicates(SQ_Shortcut_To_Product, ["PRODUCT_ID"])
        
        logging.info("Data is transformed")
        
         # Load the cleaned data into the raw.products table
        load_to_postgres(SQ_Shortcut_To_Product, "raw.products")

    except Exception as e:
        log.error(f"Products ETL failed: {str(e)}", exc_info=True)
        raise

    finally:
        spark.stop()
        log.info("Spark session stopped")

#create a task that ingests data into raw.customers table
@task
def m_ingest_data_into_customers():
    try:
        spark = create_session()
        
        # Extract customers data from API
        extractor = Extractor(CUSTOMERS_URL)
        data = extractor.extract_data()

         # Convert extracted JSON data to Spark DataFrame
        customers_df = spark.createDataFrame(data)
        
        # Rename columns
        SQ_Shortcut_To_Customer = (
              customers_df
             .withColumnRenamed("customer_id", "CUSTOMER_ID")
             .withColumnRenamed("name", "NAME")
             .withColumnRenamed("city", "CITY")
             .withColumnRenamed("email", "EMAIL")
             .withColumnRenamed("phone_number", "PHONE_NUMBER")
             .select("CUSTOMER_ID", "NAME", "CITY", "EMAIL", "PHONE_NUMBER")
)

         
        # Check for duplicate CUSTOMER_IDs
        Duplicate_check.has_duplicates(SQ_Shortcut_To_Customer, ["CUSTOMER_ID"])
        
        logging.info("Data is transformed")

         # Load the cleaned data into the raw.customers table
        load_to_postgres(SQ_Shortcut_To_Customer, "raw.customers")

    except Exception as e:
        log.error(f"Customers ETL failed: {str(e)}", exc_info=True)
        raise

    finally:
        spark.stop()
        log.info("Spark session stopped")