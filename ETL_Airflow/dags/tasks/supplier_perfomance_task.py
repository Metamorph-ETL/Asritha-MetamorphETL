from airflow.decorators import task
from utils import create_session, load_to_postgres, Duplicate_check, end_session, log, read_from_postgres
from pyspark.sql.functions import sum as sum, col, countDistinct, rank, current_date, when, lit, row_number, desc
from pyspark.sql.window import Window
from airflow.exceptions import AirflowException

@task(task_id="m_load_suppliers_perfomance")
def m_load_suppliers_perfomance():
    try:
        spark = create_session()

        #Processing Node : SQ_Shortcut_To_Sales - Reads data from 'raw.sales' table
        SQ_Shortcut_To_Sales = read_from_postgres(spark, "raw.sales") \
                                   .select(
                                       col("PRODUCT_ID"),
                                       col("QUANTITY"),
                                       col("ORDER_STATUS")
                                   )
        log.info("Data Frame : 'SQ_Shortcut_To_Sales' is built")
        
        #Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products") \
                                      .select(                                      
                                          col("PRODUCT_ID"),
                                          col("SUPPLIER_ID"),
                                          col("PRODUCT_NAME"),
                                          col("SELLING_PRICE")                                   
                                      )
        log.info("Data Frame : 'SQ_Shortcut_To_Products' is built")

        #Processing Node : SQ_Shortcut_To_Suppliers - Reads data from 'raw.suppliers' table
        SQ_Shortcut_To_Suppliers = read_from_postgres(spark, "raw.suppliers") \
                                       .select(
                                           col("SUPPLIER_ID"),
                                           col("SUPPLIER_NAME")
                                       )
        log.info("Data Frame : 'SQ_Shortcut_To_Suppliers' is built")

        #Processing Node : FIL_Sales_Cancelled - Filter out records where ORDER_STATUS is 'Cancelled'
        FIL_Sales_Cancelled = SQ_Shortcut_To_Sales \
                                  .filter(
                                      col("ORDER_STATUS") != "Cancelled"
                                  ) 
        log.info("Data Frame : 'FIL_Sales_Cancelled' is built")

        #Processing Node : JNR_Sales_Products - Join sales and products
        JNR_Sales_Products = FIL_Sales_Cancelled \
                                 .join(
                                     SQ_Shortcut_To_Products, 
                                     on="PRODUCT_ID",
                                     how="left"
                                 ) \
                                 .select(
                                     FIL_Sales_Cancelled.QUANTITY,
                                     SQ_Shortcut_To_Products.PRODUCT_ID,
                                     SQ_Shortcut_To_Products.SUPPLIER_ID,
                                     SQ_Shortcut_To_Products.PRODUCT_NAME,
                                     SQ_Shortcut_To_Products.SELLING_PRICE
                                 )
        log.info("Data Frame : 'JNR_Sales_Products' is built")
        
        #Processing Node : JNR_Products_Suppliers - Join product-sales and suppliers
        JNR_Products_Suppliers = JNR_Sales_Products \
                                     .join(
                                         SQ_Shortcut_To_Suppliers,
                                         on="SUPPLIER_ID",
                                         how="right"
                                     ) \
                                     .select(
                                         JNR_Sales_Products.PRODUCT_ID,
                                         JNR_Sales_Products.PRODUCT_NAME,
                                         JNR_Sales_Products.QUANTITY,
                                         JNR_Sales_Products.SELLING_PRICE,
                                         SQ_Shortcut_To_Suppliers.SUPPLIER_ID,
                                         SQ_Shortcut_To_Suppliers.SUPPLIER_NAME
                                     ) \
                                     .withColumn("REVENUE", col("QUANTITY") * col("SELLING_PRICE"))                          
        log.info("Data Frame : 'JNR_Products_Suppliers' is built")  
  
        #Processing Node : AGG_Supplier_Product - Aggregate revenue and quantity at supplier-product level
        AGG_Supplier_Product = JNR_Products_Suppliers \
                                   .groupBy(
                                       "SUPPLIER_ID", 
                                       "SUPPLIER_NAME", 
                                       "PRODUCT_ID", 
                                       "PRODUCT_NAME"
                                   ) \
                                   .agg(
                                       sum("REVENUE").alias("agg_REVENUE"),
                                       sum("QUANTITY").alias("agg_QUANTITY")
                                   ) 
        log.info("Data Frame : 'AGG_Supplier_Product' is built")

        #Processing Node : window_spec - Get top product per supplier by revenue
        window_spec = Window.partitionBy("SUPPLIER_ID") \
                                    .orderBy(
                                        col("agg_REVENUE").desc()
                                    )

        #Processing Node : FIL_Top_Products - Get top product with ranking
        FIL_Top_Products = AGG_Supplier_Product \
                                .withColumn("rank", row_number().over(window_spec)
                                ) \
                                .filter(
                                    col("rank") == 1
                                ) \
                                .select(
                                    "SUPPLIER_ID", 
                                    col("PRODUCT_NAME").alias("TOP_SELLING_PRODUCT")
                                )
        log.info("Data Frame : 'FIL_Top_Products ' is built")

        #Processing Node : AGG_Supplier_Level - Aggregates at supplier level
        AGG_Supplier_Level = AGG_Supplier_Product \
                                 .groupBy(
                                     "SUPPLIER_ID", 
                                     "SUPPLIER_NAME"
                                 ) \
                                 .agg(
                                     sum("agg_REVENUE").alias("TOTAL_REVENUE"),
                                     sum("agg_QUANTITY").alias("TOTAL_STOCK_SOLD"),
                                     countDistinct("PRODUCT_ID").alias("TOTAL_PRODUCTS_SOLD")
                                 )
        log.info("Data Frame : 'AGG_Supplier_Level' is built")

        #Processing Node : Shortcut_To_Supplier_Performance_Tgt - Final target dataset
        Shortcut_To_Supplier_Performance_Tgt = AGG_Supplier_Level \
                                                   .join(
                                                       FIL_Top_Products, 
                                                       on="SUPPLIER_ID", 
                                                       how="left"
                                                   ) \
                                                   .withColumn("TOP_SELLING_PRODUCT",
                                                    when(
                                                        col("TOP_SELLING_PRODUCT").isNull(), 
                                                        lit("No sales")
                                                    ) 
                                                    .otherwise(
                                                        col("TOP_SELLING_PRODUCT")
                                                    )
                                                   ) \
                                                   .withColumn("DAY_DT", current_date()) \
                                                   .fillna({
                                                        "TOTAL_REVENUE": 0,
                                                        "TOTAL_PRODUCTS_SOLD": 0,
                                                        "TOTAL_STOCK_SOLD": 0
                                                   }) \
                                                   .orderBy(desc("TOTAL_REVENUE")
                                                   ) \
                                                   .select(
                                                        col("DAY_DT"),                                                     
                                                        col("SUPPLIER_ID"),
                                                        col("SUPPLIER_NAME"),
                                                        col("TOTAL_REVENUE"),
                                                        col("TOTAL_PRODUCTS_SOLD"),
                                                        col("TOTAL_STOCK_SOLD"),
                                                        col("TOP_SELLING_PRODUCT")
                                                    )
        log.info("Data Frame : 'Shortcut_To_Supplier_Performance_Tgt' is built")

        # Check for duplicates before load
        checker = Duplicate_check()
        checker.has_duplicates(Shortcut_To_Supplier_Performance_Tgt, ["SUPPLIER_ID", "DAY_DT"])

        # Load to PostgreSQL
        load_to_postgres(Shortcut_To_Supplier_Performance_Tgt, "legacy.supplier_performance", "append")

        return "Supplier performance task completed successfully."

    except Exception as e:
        log.error(f"Supplier performance calculation failed: {str(e)}", exc_info=True)
        raise AirflowException("Supplier_performance ETL failed")

    finally:
        end_session(spark)