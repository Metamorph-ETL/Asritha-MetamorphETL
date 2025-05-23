from pyspark.sql.functions import count,col
from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import create_session, load_to_postgres, Extractor, log, Duplicate_check, end_session, read_from_postgres
from secret_key import  POSTGRES_PASSWORD
from dotenv import load_dotenv
load_dotenv()
from pyspark.sql.functions import sum, col, countDistinct, rank, current_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

#create a task that ingests data into raw.suppliers table
@task(task_id="m_ingest_data_into_suppliers")
def m_ingest_data_into_suppliers():
    try:
        spark = create_session()

        # Extract supplier data from API
        extractor = Extractor("/v1/suppliers")

        data = extractor.extract_data()
        
        # Convert extracted JSON data to Spark DataFrame
        suppliers_df = spark.createDataFrame(data)

        # Rename columns
        suppliers_df=suppliers_df \
                            .withColumnRenamed("supplier_id", "SUPPLIER_ID") \
                            .withColumnRenamed("supplier_name", "SUPPLIER_NAME") \
                            .withColumnRenamed("contact_details", "CONTACT_DETAILS") \
                            .withColumnRenamed("region", "REGION")
            
            
        suppliers_df_tgt=suppliers_df \
                            .select(
                                col("SUPPLIER_ID"),
                                col("SUPPLIER_NAME"),
                                col("CONTACT_DETAILS"),
                                col("REGION")
                            )
    
        # Check for duplicate SUPPLIER_IDs
        checker=Duplicate_check()
        checker.has_duplicates(suppliers_df_tgt, ["SUPPLIER_ID"])    

        # Load the cleaned data into the raw.suppliers table
        load_to_postgres(suppliers_df_tgt, "raw.suppliers", "overwrite")
        return "Task for loading Suppliers got completed successfully."
     
    except Exception as e:
        log.error(f"Suppliers ETL failed: {str(e)}", exc_info=True)
        raise AirflowException("Suppliers ETL failed")

    finally:
        end_session(spark)

#create a task that ingests data into raw.products table
@task(task_id="m_ingest_data_into_products")
def m_ingest_data_into_products():
    try:
        spark = create_session()
        
        # Extract products data from API
        extractor = Extractor("/v1/products")
  
        data = extractor.extract_data()
        
        # Convert extracted JSON data to Spark DataFrame
        products_df = spark.createDataFrame(data)

         # Rename columns
        products_df=products_df \
                        .withColumnRenamed("product_id", "PRODUCT_ID") \
                        .withColumnRenamed("product_name", "PRODUCT_NAME") \
                        .withColumnRenamed("category", "CATEGORY") \
                        .withColumnRenamed("selling_price", "SELLING_PRICE") \
                        .withColumnRenamed( "cost_price","COST_PRICE") \
                        .withColumnRenamed("stock_quantity", "STOCK_QUANTITY") \
                        .withColumnRenamed("reorder_level", "REORDER_LEVEL") \
                        .withColumnRenamed("supplier_id", "SUPPLIER_ID")
            

        products_df_tgt=products_df \
                                .select(
                                    col("PRODUCT_ID"),
                                    col("PRODUCT_NAME"),
                                    col("CATEGORY"),
                                    col("SELLING_PRICE"),
                                    col("COST_PRICE"),
                                    col("STOCK_QUANTITY"),
                                    col("REORDER_LEVEL"),
                                    col("SUPPLIER_ID")
                                )
        
        # Check for duplicate PRODUCT_IDs
        checker=Duplicate_check()
        checker.has_duplicates(products_df_tgt, ["PRODUCT_ID"])
       
         # Load the cleaned data into the raw.products table
        load_to_postgres(products_df_tgt, "raw.products", "overwrite")

        return "Task for loading products got completed successfully."

    except Exception as e:
        log.error(f"Products ETL failed: {str(e)}", exc_info=True)
        raise AirflowException("Products ETL failed")
    

    finally:
        end_session(spark)

#create a task that ingests data into raw.customers table
@task(task_id="m_ingest_data_into_customers")
def m_ingest_data_into_customers():
    try:
        spark = create_session()
        extractor = Extractor("/v1/customers")
        data = extractor.extract_data()
        customers_df = spark.createDataFrame(data)

        customers_df=customers_df \
                        .withColumnRenamed("customer_id", "CUSTOMER_ID") \
                        .withColumnRenamed("name", "NAME") \
                        .withColumnRenamed("city", "CITY") \
                        .withColumnRenamed("email", "EMAIL") \
                        .withColumnRenamed("phone_number", "PHONE_NUMBER") 
            
        customers_df_tgt=customers_df \
                            .select(
                                col("CUSTOMER_ID"),
                                col("NAME"),
                                col("CITY"),
                                col("EMAIL"),
                                col("PHONE_NUMBER")
                            )
        
        # Check for duplicate CUSTOMER_IDs
        checker=Duplicate_check()
        checker.has_duplicates(customers_df_tgt, ["CUSTOMER_ID"])

         # Load the cleaned data into the raw.customers table
        load_to_postgres(customers_df_tgt, "raw.customers", "overwrite")
        return "Task for loading customers got completed successfully."

    except Exception as e:
        log.error(f"Customers ETL failed: {str(e)}", exc_info=True)
        raise AirflowException("Customers ETL failed")
    

    finally:
        end_session(spark)

#create a task that ingests data into raw.sales table
@task(task_id="m_ingest_data_into_sales")
def m_ingest_data_into_sales():
    try:
        spark=create_session()
        # Define the GCS bucket name
        GCS_BUCKET_NAME = "meta-morph"
        today_str= "20250322"

        #GCS path to the sales CSV file for today's date
        gcs_path = f"gs://{GCS_BUCKET_NAME}/{today_str}/sales_{today_str}.csv"
        log.info(f"Reading CSV file from GCS: {gcs_path}")
        
        # Read the CSV file from GCS into a DataFrame
        sales_df= spark.read.csv(gcs_path, header=True, inferSchema=True)
        log.info(f"CSV loaded successfully. Number of rows: {sales_df.count()}")
        
        # Rename columns to match schema standards (uppercase), and select the required columns
        sales_df=sales_df \
                    .withColumnRenamed("sale_id", "SALE_ID") \
                    .withColumnRenamed("customer_id", "CUSTOMER_ID") \
                    .withColumnRenamed("product_id", "PRODUCT_ID") \
                    .withColumnRenamed("sale_date", "SALE_DATE") \
                    .withColumnRenamed("quantity", "QUANTITY") \
                    .withColumnRenamed("discount", "DISCOUNT") \
                    .withColumnRenamed("shipping_cost", "SHIPPING_COST") \
                    .withColumnRenamed("order_status", "ORDER_STATUS") \
                    .withColumnRenamed("payment_mode", "PAYMENT_MODE") 
            
            
        sales_df_tgt=sales_df \
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
    
         
        # Check for duplicates based on SALE_ID column
        checker=Duplicate_check()
        checker.has_duplicates(sales_df_tgt, ["SALE_ID"])

        #writing data to PostgreSQL
        load_to_postgres(sales_df_tgt, "raw.sales", "overwrite")
        return "Task for loading Sales got completed successfully."
   
    except Exception as e:
        log.error(f"Error occurred: {e}")
        raise AirflowException("sales ETL failed")
    
    finally:
        end_session(spark)
