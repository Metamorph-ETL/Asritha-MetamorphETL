from airflow.decorators import task
from utils import create_session, load_to_postgres, Duplicate_check, end_session, log, read_from_postgres
from pyspark.sql.functions import sum, col, countDistinct, rank, current_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


@task
def m_calculate_supplier_perfomance():
    try:
        spark = create_session()

        # Read necessary tables
        sales_df = read_from_postgres(spark, "raw.sales")
        products_df = read_from_postgres(spark, "raw.products")
        suppliers_df = read_from_postgres(spark, "raw.suppliers")

        log.info("Successfully read tables from PostgreSQL")

        # Check for duplicate sales IDs
        checker = Duplicate_check()
        checker.has_duplicates(sales_df, ["SALE_ID"])

        # Filter out cancelled orders
        sales_df = sales_df.filter(col("ORDER_STATUS") != "Cancelled")

        # Join sales with products and suppliers
        sales_products = (sales_df.join(products_df, "PRODUCT_ID", "left").join(suppliers_df, "SUPPLIER_ID", "left"))

        # Calculate revenue
        sales_products = sales_products.withColumn("REVENUE", col("QUANTITY") * col("SELLING_PRICE"))

        # Supplier-level metrics
        supplier_metrics = sales_products.groupBy("SUPPLIER_ID") \
                            .agg(sum(col("REVENUE")).alias("TOTAL_REVENUE"),
                            sum("QUANTITY").alias("TOTAL_STOCK_SOLD"),
                            countDistinct("PRODUCT_ID").alias("TOTAL_PRODUCTS_SOLD"))

        # Top-selling product per supplier (based on REVENUE)

        window_spec = Window.partitionBy("SUPPLIER_ID").orderBy(col("REVENUE").desc(), col("PRODUCT_NAME"))

        top_products = sales_products.withColumn("row_num", row_number().over(window_spec)) \
                          .filter(col("row_num") == 1) \
                          .select(
                               col("SUPPLIER_ID"),
                               col("PRODUCT_NAME").alias("TOP_SELLING_PRODUCT"))
 
        # Final result
        final_df = supplier_metrics \
                        .join(suppliers_df.select("SUPPLIER_ID", "SUPPLIER_NAME"), "SUPPLIER_ID") \
                        .join(top_products, "SUPPLIER_ID") \
                        .withColumn("DAY_DT", current_date()) \
                        .select(
                            col("DAY_DT"),
                            col("SUPPLIER_ID"),
                            col("SUPPLIER_NAME"),
                            col("TOTAL_REVENUE"),
                            col("TOTAL_PRODUCTS_SOLD"),
                            col("TOTAL_STOCK_SOLD"),
                            col("TOP_SELLING_PRODUCT"))

        #  Check for duplicates per supplier/day
        checker.has_duplicates(final_df, ["SUPPLIER_ID", "DAY_DT"])

        # Load to PostgreSQL (append mode)
        load_to_postgres(final_df, "legacy.Supplier_Performance", mode="append")

        log.info("Loaded Supplier Performance successfully.")
        return "Supplier performance calculation completed successfully."

    except Exception as e:
        log.error(f"Supplier performance calculation failed: {str(e)}", exc_info=True)

    finally:
        end_session(spark) 