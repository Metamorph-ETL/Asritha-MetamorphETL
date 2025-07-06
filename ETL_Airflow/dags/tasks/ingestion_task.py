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
from datetime import datetime

# Create a task that ingests data into raw.suppliers table
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
        suppliers_df = suppliers_df \
                        .withColumnRenamed("Supplier Id", "SUPPLIER_ID") \
                        .withColumnRenamed("Supplier Name", "SUPPLIER_NAME") \
                        .withColumnRenamed("Contact Details", "CONTACT_DETAILS") \
                        .withColumnRenamed("Region", "REGION")
            
        # Selecting required columns from the source DataFrame `suppliers_df`    
        suppliers_df_tgt = suppliers_df \
                                .select(
                                    col("SUPPLIER_ID"),
                                    col("SUPPLIER_NAME"),
                                    col("CONTACT_DETAILS"),
                                    col("REGION")
                                )

        # Adding a column "DAY_DT" with the current date to track daily snapshots
        suppliers_legacy_df = suppliers_df_tgt \
                               .withColumn("DAY_DT", current_date())

        # Rearranging and selecting final columns for writing to the legacy table
        suppliers_legacy_df_tgt = suppliers_legacy_df \
                                    .select(
                                        col("DAY_DT"),
                                        col("SUPPLIER_ID"),
                                        col("SUPPLIER_NAME"),
                                        col("CONTACT_DETAILS"),
                                        col("REGION")
                                    )
    
        # Check for duplicate SUPPLIER_IDs
        checker = Duplicate_check()
        checker.has_duplicates(suppliers_df_tgt, ["SUPPLIER_ID"])    

        # Load the cleaned data into the raw.suppliers table
        load_to_postgres(suppliers_df_tgt, "raw.suppliers_pre", "overwrite")

        # Load the cleaned data into the legacy.suppliers table
        load_to_postgres(suppliers_legacy_df_tgt, "legacy.suppliers", "append")
        
        return "Task for loading Suppliers got completed successfully."
     
    except Exception as e:
        log.error(f"Suppliers ETL failed: {str(e)}", exc_info=True)
        raise AirflowException("Suppliers ETL failed")

    finally:
        end_session(spark)

# Create a task that ingests data into raw.products table
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
        products_df = products_df \
                        .withColumnRenamed("Product Id", "PRODUCT_ID") \
                        .withColumnRenamed("Product Name", "PRODUCT_NAME") \
                        .withColumnRenamed("Category", "CATEGORY") \
                        .withColumnRenamed("Selling Price", "SELLING_PRICE") \
                        .withColumnRenamed("Cost Price", "COST_PRICE") \
                        .withColumnRenamed("Stock Quantity", "STOCK_QUANTITY") \
                        .withColumnRenamed("Reorder Level", "REORDER_LEVEL") \
                        .withColumnRenamed("Supplier Id", "SUPPLIER_ID")

            
        # Selecting required columns from the source DataFrame `products_df`
        products_df_tgt = products_df \
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
        
        # Adding a column "DAY_DT" with the current date to track daily snapshots
        products_legacy_df = products_df_tgt \
                               .withColumn("DAY_DT", current_date())

        # Rearranging and selecting final columns for writing to the legacy table
        products_legacy_df_tgt = products_legacy_df \
                                    .select(
                                        col("DAY_DT"),
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
        checker = Duplicate_check()
        checker.has_duplicates(products_df_tgt, ["PRODUCT_ID"])
       
        # Load the cleaned data into the raw.products table
        load_to_postgres(products_df_tgt, "raw.products_pre", "overwrite")

        # Load the cleaned data into the legacy.products table
        load_to_postgres(products_legacy_df_tgt, "legacy.products", "append")

        return "Task for loading products got completed successfully."

    except Exception as e:
        log.error(f"Products ETL failed: {str(e)}", exc_info=True)
        raise AirflowException("Products ETL failed")
    
    finally:
        end_session(spark)

# Create a task that ingests data into raw.customers table
@task(task_id="m_ingest_data_into_customers")
def m_ingest_data_into_customers():
    try:
        spark = create_session()

        # Extract customers data from API
        extractor = Extractor("/v1/customers")
        data = extractor.extract_data()

        # Convert extracted JSON data to Spark DataFrame
        customers_df = spark.createDataFrame(data)

        # Rename columns
        customers_df = customers_df \
                            .withColumnRenamed("Customer Id", "CUSTOMER_ID") \
                            .withColumnRenamed("Name", "NAME") \
                            .withColumnRenamed("City", "CITY") \
                            .withColumnRenamed("Email", "EMAIL") \
                            .withColumnRenamed("Phone Number", "PHONE_NUMBER")


        # Selecting required columns from the source DataFrame `customers_df`  
        customers_df_tgt = customers_df \
                                .select(
                                    col("CUSTOMER_ID"),
                                    col("NAME"),
                                    col("CITY"),
                                    col("EMAIL"),
                                    col("PHONE_NUMBER")
                                )

        # Adding a column "DAY_DT" with the current date to track daily snapshots                   
        customers_legacy_df = customers_df_tgt \
                               .withColumn("DAY_DT", current_date())
        
        # Rearranging and selecting final columns for writing to the legacy table          
        customers_legacy_df_tgt = customers_legacy_df \
                                    .select(
                                        col("DAY_DT"),
                                        col("CUSTOMER_ID"),
                                        col("NAME"),
                                        col("CITY"),
                                        col("EMAIL"),
                                        col("PHONE_NUMBER")
                                    )

        # Check for duplicate CUSTOMER_IDs
        checker = Duplicate_check()
        checker.has_duplicates(customers_df_tgt, ["CUSTOMER_ID"])

         # Load the cleaned data into the raw.customers table
        load_to_postgres(customers_df_tgt, "raw.customers_pre", "overwrite")

        # Load the cleaned data into the legacy.customers table
        load_to_postgres(customers_legacy_df_tgt, "legacy.customers", "append")

        return "Task for loading customers got completed successfully."

    except Exception as e:
        log.error(f"Customers ETL failed: {str(e)}", exc_info=True)
        raise AirflowException("Customers ETL failed")
    
    finally:
        end_session(spark)

# Create a task that ingests data into raw.sales table
@task(task_id="m_ingest_data_into_sales")
def m_ingest_data_into_sales():
    try:
        spark=create_session()

        # Define the GCS bucket name
        GCS_BUCKET_NAME = "meta-morph-flow"
        today_str = datetime.today().strftime("%Y%m%d")

        # GCS path to the sales CSV file for today's date
        gcs_path = f"gs://{GCS_BUCKET_NAME}/{today_str}/sales_{today_str}.csv"
        log.info(f"Reading CSV file from GCS: {gcs_path}")
        
        # Read the CSV file from GCS into a DataFrame
        sales_df = spark.read.csv(gcs_path, header=True, inferSchema=True)
        log.info(f"CSV loaded successfully. Number of rows: {sales_df.count()}")
        
        # Rename columns
        sales_df = sales_df \
                        .withColumnRenamed("Sale Id", "SALE_ID") \
                        .withColumnRenamed("Customer Id", "CUSTOMER_ID") \
                        .withColumnRenamed("Product Id", "PRODUCT_ID") \
                        .withColumnRenamed("Sale Date", "SALE_DATE") \
                        .withColumnRenamed("Quantity", "QUANTITY") \
                        .withColumnRenamed("Discount", "DISCOUNT") \
                        .withColumnRenamed("Shipping Cost", "SHIPPING_COST") \
                        .withColumnRenamed("Order Status", "ORDER_STATUS") \
                        .withColumnRenamed("Payment Mode", "PAYMENT_MODE")

                
        # Selecting required columns from the source DataFrame `sales_df`    
        sales_df_tgt = sales_df \
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

        # Adding a column "DAY_DT" with the current date to track daily snapshots
        sales_legacy_df = sales_df_tgt \
                               .withColumn("DAY_DT", current_date())
         
        # Rearranging and selecting final columns for writing to the legacy table
        sales_legacy_df_tgt = sales_legacy_df \
                                 .select(
                                     col("DAY_DT"),
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
        checker = Duplicate_check()
        checker.has_duplicates(sales_df_tgt, ["SALE_ID"])

        # Load the cleaned data into the raw.sales table
        load_to_postgres(sales_df_tgt, "raw.sales_pre", "overwrite")

        # Load the cleaned data into the legacy.sales table
        load_to_postgres(sales_legacy_df_tgt, "legacy.sales", "append")
        
        return "Task for loading Sales got completed successfully."
   
    except Exception as e:
        log.error(f"Error occurred: {e}")
        raise AirflowException("sales ETL failed")
    
    finally:
        end_session(spark)
