from airflow.decorators import task
from utils import create_session,read_from_postgres,end_session,Duplicate_check,load_to_postgres,log
from pyspark.sql.functions import col,current_date,sum,avg,when
from pyspark.sql.window import Window
from airflow.exceptions import AirflowException

@task(task_id="m_load_products_perfomance")
def m_load_products_perfomance():
    try:
        spark = create_session()
        
        #Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products") \
                                        .select(
                                            col("PRODUCT_ID"),
                                            col("PRODUCT_NAME"),
                                            col("SELLING_PRICE"),
                                            col("CATEGORY"),
                                            col("COST_PRICE"),
                                            col("STOCK_QUANTITY"),
                                            col("REORDER_LEVEL")
                                        )
        log.info("Data Frame : 'SQ_Shortcut_To_Products' is built")

        #Processing Node : SQ_Shortcut_To_Sales - Reads data from 'raw.Sales' table
        SQ_Shortcut_To_Sales = read_from_postgres(spark, "raw.sales") \
                                    .select(
                                        col("PRODUCT_ID"),
                                        col("QUANTITY"),
                                    )
        log.info("Data Frame : 'SQ_Shortcut_To_Sales' is built")
        
        #Processing Node : JNR_Sales_Products - Reads data from 'raw.products' and 'raw.sales' tables and calculate metrics
        JNR_Sales_Products = SQ_Shortcut_To_Products \
                                .join(
                                    SQ_Shortcut_To_Sales, 
                                    on="PRODUCT_ID",
                                    how="left"
                                ) \
                                .select(
                                    SQ_Shortcut_To_Products.PRODUCT_ID,
                                    SQ_Shortcut_To_Products.PRODUCT_NAME,
                                    SQ_Shortcut_To_Products.CATEGORY,
                                    SQ_Shortcut_To_Sales.QUANTITY,
                                    SQ_Shortcut_To_Products.SELLING_PRICE,
                                    SQ_Shortcut_To_Products.COST_PRICE,
                                    SQ_Shortcut_To_Products.STOCK_QUANTITY
                                ) \
                                .withColumn("TOTAL_SALES_AMOUNT", col("QUANTITY") * col("SELLING_PRICE")) \
                                .withColumn("PROFIT_AMOUNT", col("QUANTITY") * (col("SELLING_PRICE") - col("COST_PRICE")))
        log.info("Data Frame : 'JNR_Sales_Products' is built")

        # Processing Node :  AGG_Product_Performance - Aggregate sales metrics by product
        AGG_Product_Performance = JNR_Sales_Products \
                                     .groupBy("PRODUCT_ID", "PRODUCT_NAME", "CATEGORY") \
                                     .agg(
                                     sum("QUANTITY").alias("agg_QUANTITY"),
                                     sum("TOTAL_SALES_AMOUNT").alias("agg_TOTAL_SALES_AMOUNT"),
                                     avg("SELLING_PRICE").alias("agg_SELLING_PRICE"),
                                     sum("PROFIT_AMOUNT").alias("agg_PROFIT_AMOUNT")
                                     ) 
        log.info("Aggregated product performance metrics calculated")
        
        # Processing Node : JNR_Product_Agg_Perfomance- Aggregates data at the product level 
        JNR_Product_Agg_Perfomance = AGG_Product_Performance \
                                            .join(
                                                SQ_Shortcut_To_Products.select("PRODUCT_ID", "STOCK_QUANTITY", "REORDER_LEVEL"),
                                                on="PRODUCT_ID",
                                                how="inner"
                                            ) \
                                            .withColumn("STOCK_LEVEL_STATUS",when(col("STOCK_QUANTITY") <= col("REORDER_LEVEL"), "Below Reorder Level").otherwise("Sufficient Stock")) \
                                            .withColumn("DAY_DT", current_date()) 
        
        #Processing Node : Shortcut_To_Product_Performance_Tgt - Final target dataset
        Shortcut_To_Product_Performance_Tgt =  JNR_Product_Agg_Perfomance \
                                                    .select(
                                                            col("DAY_DT"),
                                                            col("PRODUCT_ID"),
                                                            col("PRODUCT_NAME"),
                                                            col("agg_TOTAL_SALES_AMOUNT"),
                                                            col("agg_QUANTITY"),
                                                            col("agg_SELLING_PRICE"),
                                                            col("STOCK_QUANTITY"),
                                                            col("REORDER_LEVEL"),
                                                            col("STOCK_LEVEL_STATUS"),
                                                            col("agg_PROFIT_AMOUNT"),
                                                            col("CATEGORY")
                                                    )                                                                           
        log.info("Final Data Frame : 'Shortcut_To_Product_Performance_Tgt' is built")
                                             
        # Check for duplicates before load
        checker = Duplicate_check()
        checker.has_duplicates(Shortcut_To_Product_Performance_Tgt , ["PRODUCT_ID", "DAY_DT"])
        
        # Load to PostgreSQL
        load_to_postgres(Shortcut_To_Product_Performance_Tgt, "legacy.product_performance", "append")

    except Exception as e:
        log.error(f"Product performance calculation failed: {str(e)}", exc_info=True)
        raise AirflowException("product_performance ETL failed")

    finally:
        end_session(spark)
