from pyspark.sql.functions import count,col
from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import create_session, load_to_postgres, Extractor, log,Duplicate_check
from secret_key import SUPPLIERS_URL, PRODUCTS_URL, CUSTOMERS_URL,TOKEN_URL, POSTGRES_PASSWORD
from dotenv import load_dotenv
load_dotenv()

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
            .select(
                col("SUPPLIER_ID"),
                col("SUPPLIER_NAME"),
                col("CONTACT_DETAILS"),
                col("REGION")
            )
        )
        
        # Check for duplicate SUPPLIER_IDs
        Duplicate_check.has_duplicates(SQ_Shortcut_To_Supplier, ["SUPPLIER_ID"])
        
        log.info(f"suppliers data cleaned and ready for loading : {SQ_Shortcut_To_Supplier.count()} records processed")
        
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
            .select(
                col("PRODUCT_ID"),
                col("PRODUCT_NAME"),
                col("CATEGORY"),
                col("PRICE"),
                col("STOCK_QUANTITY"),
                col("REORDER_LEVEL"),
                col("SUPPLIER_ID")
            )
        )
        
        
        # Check for duplicate PRODUCT_IDs
        Duplicate_check.has_duplicates(SQ_Shortcut_To_Product, ["PRODUCT_ID"])
        
        log.info(f"products data cleaned and ready for loading : {SQ_Shortcut_To_Product.count()} records processed")
        
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
        extractor = Extractor(CUSTOMERS_URL)
        data = extractor.extract_data()
        customers_df = spark.createDataFrame(data)

        SQ_Shortcut_To_Customer= (
            customers_df
            .withColumnRenamed("customer_id", "CUSTOMER_ID")
            .withColumnRenamed("name", "NAME")
            .withColumnRenamed("city", "CITY")
            .withColumnRenamed("email", "EMAIL")
            .withColumnRenamed("phone_number", "PHONE_NUMBER")
            .select(
                col("CUSTOMER_ID"),
                col("NAME"),
                col("CITY"),
                col("EMAIL"),
                col("PHONE_NUMBER")
            )
        )

   
        # Check for duplicate CUSTOMER_IDs
        Duplicate_check.has_duplicates(SQ_Shortcut_To_Customer, ["CUSTOMER_ID"])
        
        log.info(f"products data cleaned and ready for loading : {SQ_Shortcut_To_Customer.count()} records processed")

         # Load the cleaned data into the raw.customers table
        load_to_postgres(SQ_Shortcut_To_Customer, "raw.customers")

    except Exception as e:
        log.error(f"Customers ETL failed: {str(e)}", exc_info=True)
        raise

    finally:
        spark.stop()
        log.info("Spark session stopped")

#create a task that ingests data into raw.sales table
@task
def m_ingest_data_into_sales(today_str: str = "20250322"):
    try:
        spark=create_session()
        # Define the GCS bucket name
        GCS_BUCKET_NAME = "meta-morph"

        #GCS path to the sales CSV file for today's date
        gcs_path = f"gs://{GCS_BUCKET_NAME}/{today_str}/sales_{today_str}.csv"
        log.info(f"Reading CSV file from GCS: {gcs_path}")
        
        # Read the CSV file from GCS into a DataFrame
        sales_df= spark.read.csv(gcs_path, header=True, inferSchema=True)
        log.info(f"CSV loaded successfully. Number of rows: {sales_df.count()}")
        
        # Rename columns to match schema standards (uppercase), and select the required columns
        SQ_Shortcut_To_Sale= (
            sales_df
            .withColumnRenamed("sale_id", "SALE_ID")
            .withColumnRenamed("customer_id", "CUSTOMER_ID")
            .withColumnRenamed("product_id", "PRODUCT_ID")
            .withColumnRenamed("sale_date", "SALE_DATE")
            .withColumnRenamed("quantity", "QUANTITY")
            .withColumnRenamed("discount", "DISCOUNT")
            .withColumnRenamed("shipping_cost", "SHIPPING_COST")
            .withColumnRenamed("order_status", "ORDER_STATUS")
            .withColumnRenamed("payment_mode", "PAYMENT_MODE")
            .select(
                col("SALE_ID"),
                col("CUSTOMER_ID"),
                col("PRODUCT_ID"),
                col("SALE_DATE"),
                col("QUANTITY"),
                col("DISCOUNT"),
                col("SHIPPING_COST"),
                col("ORDER_STATUS"),
                col("PAYMENT_MODE")
            )
        )
         
        # Check for duplicates based on SALE_ID column
        Duplicate_check.has_duplicates(SQ_Shortcut_To_Sale, ["SALE_ID"])
        log.info("Dropping duplicates based on SALE_ID column...")

         # Drop duplicates from the DataFrame
        SQ_Shortcut_To_Sale= SQ_Shortcut_To_Sale.dropDuplicates(["SALE_ID"])


        log.info(f"Number of rows after duplicates dropped: {SQ_Shortcut_To_Sale.count()}")

        log.info("Writing data to PostgreSQL (raw.sales)...")
        

        #writing data to PostgreSQL
        load_to_postgres(SQ_Shortcut_To_Sale, "raw.sales")
   
    except Exception as e:
        log.error(f"Error occurred: {e}")
    
    finally:
        log.info("Stopping Spark session...")
        spark.stop()
        log.info("Spark session stopped.")
