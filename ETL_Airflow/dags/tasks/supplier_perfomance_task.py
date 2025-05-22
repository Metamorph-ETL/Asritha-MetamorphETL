from airflow.decorators import task
from utils import create_session, load_to_postgres, Duplicate_check, end_session, log, read_from_postgres
from pyspark.sql.functions import sum, col, countDistinct, rank, current_date, when, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from airflow.exceptions import AirflowException

@task
def m_calculate_supplier_perfomance():
    try:
        spark = create_session()

        # Read necessary tables
        SQ_Shortcut_To_Sales= read_from_postgres(spark, "raw.sales")
        SQ_Shortcut_To_Products= read_from_postgres(spark, "raw.products")
        SQ_Shortcut_To_Suppliers = read_from_postgres(spark, "raw.suppliers")

        log.info("Successfully read tables from PostgreSQL")

        # Check for duplicate sales IDs
        checker = Duplicate_check()
        checker.has_duplicates( SQ_Shortcut_To_Sales, ["SALE_ID"])

        # Filter out cancelled orders
        SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales.filter(col("ORDER_STATUS") != "Cancelled")

        # Join sales with products and suppliers
        JNR_Sales_Products_Suppliers=(SQ_Shortcut_To_Sales.join(SQ_Shortcut_To_Products, "PRODUCT_ID", "left")
                                     .join(SQ_Shortcut_To_Suppliers, "SUPPLIER_ID", "right")  # Keep all suppliers
                                     )
        
        # Calculate REVENUE 
        JNR_Sales_Products_Suppliers= JNR_Sales_Products_Suppliers.withColumn("REVENUE", col("QUANTITY") * col("SELLING_PRICE"))
  
        # Aggregate metrics per supplier
        supplier_metrics =  JNR_Sales_Products_Suppliers.groupBy("SUPPLIER_ID").agg(
                                sum("REVENUE").alias("TOTAL_REVENUE"),
                                sum("QUANTITY").alias("TOTAL_STOCK_SOLD"),
                                countDistinct("PRODUCT_ID").alias("TOTAL_PRODUCTS_SOLD"))

        window_spec = Window.partitionBy("SUPPLIER_ID").orderBy(col("REVENUE").desc(), col("PRODUCT_NAME"))

        Top_Selling_Product_df =(JNR_Sales_Products_Suppliers.withColumn("row_num", row_number().over(window_spec))
                                    .filter(col("row_num") == 1)
                                    .select("SUPPLIER_ID", col("PRODUCT_NAME").alias("TOP_SELLING_PRODUCT"))
                                )

        log.info("Final dataframe is created")
        
        # Final result
        Shortcut_To_Supplier_Performance_Tgt = (SQ_Shortcut_To_Suppliers.select("SUPPLIER_ID", "SUPPLIER_NAME")
                                                    .join(supplier_metrics, "SUPPLIER_ID", "left")
                                                    .join(Top_Selling_Product_df, "SUPPLIER_ID", "left")
                                                    .withColumn("DAY_DT", current_date())
                                                    .select(
                                                        col("DAY_DT"),
                                                        col("SUPPLIER_ID"),
                                                        col("SUPPLIER_NAME"),
                                                        col("TOTAL_REVENUE"),
                                                        col("TOTAL_PRODUCTS_SOLD"),
                                                        col("TOTAL_STOCK_SOLD"),
                                                        when(col("TOP_SELLING_PRODUCT").isNull(), lit("No sales"))
                                                        .otherwise(col("TOP_SELLING_PRODUCT"))
                                                        .alias("TOP_SELLING_PRODUCT")
                                                    )
                                                )

        #Final null filling for numeric columns and order by revenue
        Shortcut_To_Supplier_Performance_Tgt= Shortcut_To_Supplier_Performance_Tgt.fillna({
                                                  "TOTAL_REVENUE": 0,
                                                  "TOTAL_PRODUCTS_SOLD": 0,
                                                  "TOTAL_STOCK_SOLD": 0
                                                }).orderBy(col("TOTAL_REVENUE").desc())  

        # Check for duplicates before load
        checker.has_duplicates(Shortcut_To_Supplier_Performance_Tgt, ["SUPPLIER_ID", "DAY_DT"])

        # Load to PostgreSQL
        load_to_postgres(Shortcut_To_Supplier_Performance_Tgt, "legacy.Supplier_Performance", mode="append")

        log.info("Loaded Supplier Performance successfully.")
        return "Supplier performance calculation completed successfully."

    except Exception as e:
        log.error(f"Supplier performance calculation failed: {str(e)}", exc_info=True)
        raise AirflowException("Supplier_performance ETL failed")

    finally:
        end_session(spark)
