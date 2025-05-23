from airflow.decorators import task
from utils import create_session, load_to_postgres, Duplicate_check, end_session, log, read_from_postgres
from pyspark.sql.functions import sum, col, countDistinct, rank, current_date, when, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from airflow.exceptions import AirflowException

@task(task_id="m_load_suppliers_perfomance")
def m_load_suppliers_perfomance():
    try:
        spark = create_session()

        # Read necessary tables
        SQ_Shortcut_To_Sales =  read_from_postgres(spark, "raw.sales")\
                                .select(
                                    col("PRODUCT_ID"),
                                    col("QUANTITY"),
                                    col("ORDER_STATUS")
                                )
        log.info("Data Frame : 'SQ_Shortcut_To_Sales' is built")
        
        SQ_Shortcut_To_Products= read_from_postgres(spark, "raw.products")\
                                .select(                                      
                                    col("PRODUCT_ID"),
                                    col("SUPPLIER_ID"),
                                    col("PRODUCT_NAME"),
                                    col("SELLING_PRICE")                                   
                                 )
        log.info("Data Frame : 'SQ_Shortcut_To_Products' is built")

        SQ_Shortcut_To_Suppliers = read_from_postgres(spark, "raw.suppliers")\
                                   .select(
                                       col("SUPPLIER_ID"),
                                       col("SUPPLIER_NAME")
                                   )
        log.info("Data Frame : 'SQ_Shortcut_To_Suppliers' is built")

        # Filter out cancelled orders
        FIL_Sales_Cancelled= SQ_Shortcut_To_Sales.filter(col("ORDER_STATUS") != "Cancelled")
        
        log.info("Data Frame : 'FIL_Cancelled_Sales' is built")

        # Join sales with products and suppliers

        JNR_Sales_Products = FIL_Sales_Cancelled.join(SQ_Shortcut_To_Products, "PRODUCT_ID", "left")\
                             .select(
                                FIL_Sales_Cancelled["QUANTITY"],
                                SQ_Shortcut_To_Products["PRODUCT_ID"],
                                SQ_Shortcut_To_Products["SUPPLIER_ID"],
                                SQ_Shortcut_To_Products["PRODUCT_NAME"],
                                SQ_Shortcut_To_Products["SELLING_PRICE"]
                             )
        log.info("Data Frame : 'JNR_Sales_Products' is built")

        JNR_Products_Suppliers = (JNR_Sales_Products.join(SQ_Shortcut_To_Suppliers, "SUPPLIER_ID", "right")\
                                 .select(
                                        JNR_Sales_Products["PRODUCT_ID"],
                                        JNR_Sales_Products["PRODUCT_NAME"],
                                        JNR_Sales_Products["QUANTITY"],
                                        JNR_Sales_Products["SELLING_PRICE"],
                                        SQ_Shortcut_To_Suppliers["SUPPLIER_ID"],
                                        SQ_Shortcut_To_Suppliers["SUPPLIER_NAME"]
                                 ).withColumn("REVENUE", col("QUANTITY") * col("SELLING_PRICE")))
                                
        log.info("Data Frame : 'JNR_Products_Suppliers' is built")  
  
        # Aggregate metrics per supplier
        AGG_TRANS_Product =  JNR_Products_Suppliers.groupBy("SUPPLIER_ID")\
                             .agg(
                                sum("REVENUE").alias("agg_total_revenue"),
                                sum("QUANTITY").alias("agg_total_quantity"),
                                countDistinct("PRODUCT_ID").alias("TOTAL_PRODUCTS_SOLD"))
        
        log.info("Data Frame : 'AGG_TRANS_Product' is built")

        window_spec = Window.partitionBy("SUPPLIER_ID").orderBy(col("REVENUE").desc(), col("PRODUCT_NAME"))

        Top_Selling_Product_df = (JNR_Products_Suppliers.withColumn("row_num", row_number().over(window_spec))\
                                 .filter(col("row_num") == 1)\
                                 .select(col("SUPPLIER_ID"), col("PRODUCT_NAME").alias("TOP_SELLING_PRODUCT"))
                                 )
        log.info("Data Frame : 'Top_Selling_Product_df' is built")
        # Final result
        Shortcut_To_Supplier_Performance_Tgt =  (SQ_Shortcut_To_Suppliers.select("SUPPLIER_ID", "SUPPLIER_NAME")
                                                .join(AGG_TRANS_Product, "SUPPLIER_ID", "left")
                                                .join(Top_Selling_Product_df, "SUPPLIER_ID", "left")
                                                .withColumn("DAY_DT", current_date())
                                                .select(
                                                    col("DAY_DT"),
                                                    col("SUPPLIER_ID"),
                                                    col("SUPPLIER_NAME"),
                                                    col("agg_total_revenue").alias("TOTAL_REVENUE"),
                                                    col("TOTAL_PRODUCTS_SOLD"),
                                                    col("agg_total_quantity").alias("TOTAL_STOCK_SOLD"),
                                                    when(col("TOP_SELLING_PRODUCT").isNull(), lit("No sales"))
                                                    .otherwise(col("TOP_SELLING_PRODUCT"))
                                                    .alias("TOP_SELLING_PRODUCT"))
                                                )\
                                                .fillna({
                                                  "TOTAL_REVENUE": 0,
                                                  "TOTAL_PRODUCTS_SOLD": 0,
                                                  "TOTAL_STOCK_SOLD": 0
                                                })\
                                                .orderBy(col("TOTAL_REVENUE").desc())  

        log.info("Data Frame : 'Shortcut_To_Supplier_Performance_Tgt' is built")

        # Check for duplicates before load
        checker = Duplicate_check()
        checker.has_duplicates(Shortcut_To_Supplier_Performance_Tgt, ["SUPPLIER_ID", "DAY_DT"])

        # Load to PostgreSQL
        load_to_postgres(Shortcut_To_Supplier_Performance_Tgt, "legacy.Supplier_Performance", "append")

        return "Supplier performance task completed successfully."

    except Exception as e:
        log.error(f"Supplier performance calculation failed: {str(e)}", exc_info=True)
        raise AirflowException("Supplier_performance ETL failed")

    finally:
        end_session(spark)
