from airflow.decorators import task
from utils import create_session,read_from_postgres,end_session,Duplicate_check,load_to_postgres,log
from pyspark.sql.functions import col,current_date,sum,avg,when
from pyspark.sql.window import Window
from airflow.exceptions import AirflowException

@task(task_id="m_load_products_performance")
def m_load_products_performance():
    try:
        spark = create_session()
        
        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products' table
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

        # Processing Node : SQ_Shortcut_To_Sales - Reads data from 'raw.sales' table
        SQ_Shortcut_To_Sales = read_from_postgres(spark, "raw.sales") \
                                    .select(
                                        col("PRODUCT_ID"),
                                        col("QUANTITY"),
                                    )
        log.info("Data Frame : 'SQ_Shortcut_To_Sales' is built")
        
        # Processing Node : JNR_Sales_Products - Joins data from 'raw.products' and 'raw.sales' tables and calculate metrics
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
                                    .withColumn("SALES_AMOUNT", col("QUANTITY") * col("SELLING_PRICE")) \
                                    .withColumn("PROFIT_AMOUNT", col("QUANTITY") * (col("SELLING_PRICE") - col("COST_PRICE"))) 
        log.info("Data Frame : 'JNR_Sales_Products' is built")

        # Processing Node :  AGG_Product_Performance - Aggregate sales metrics by product
        AGG_Product_Performance = JNR_Sales_Products \
                                        .groupBy(
                                            "PRODUCT_ID", 
                                            "PRODUCT_NAME",
                                            "CATEGORY"
                                        ) \
                                        .agg(
                                            sum("QUANTITY").alias("agg_QUANTITY"),
                                            sum("SALES_AMOUNT").alias("agg_TOTAL_SALES_AMOUNT"),
                                            avg("SELLING_PRICE").alias("agg_SELLING_PRICE"),
                                            sum("PROFIT_AMOUNT").alias("agg_PROFIT_AMOUNT")
                                        ) 
        log.info("Data Frame : 'AGG_Product_Performance' is built")
        
        # Processing Node : JNR_Product_Agg_Perfomance- Aggregates data at the product level and join tables AGG_Product_Performance,SQ_Shortcut_To_Products
        JNR_Product_Agg_Performance = AGG_Product_Performance.alias("AGG") \
                                            .join(
                                                SQ_Shortcut_To_Products.alias("PROD"),
                                                on="PRODUCT_ID",
                                                how="inner"
                                            ) \
                                            .select(
                                                    col("AGG.PRODUCT_ID"),
                                                    col("AGG.PRODUCT_NAME"),
                                                    col("AGG.CATEGORY"),
                                                    col("AGG.agg_TOTAL_SALES_AMOUNT").alias("TOTAL_SALES_AMOUNT"),
                                                    col("AGG.agg_QUANTITY").alias("TOTAL_QUANTITY_SOLD"),
                                                    col("AGG.agg_SELLING_PRICE").alias("AVG_SALE_PRICE"),
                                                    col("AGG.agg_PROFIT_AMOUNT").alias("PROFIT"),
                                                    col("PROD.STOCK_QUANTITY"),
                                                    col("PROD.REORDER_LEVEL")
                                            ) \
                                            .withColumn("STOCK_LEVEL_STATUS",
                                            when(col("STOCK_QUANTITY") <= col("REORDER_LEVEL"), "Below Reorder Level")
                                            .otherwise("Sufficient Stock")) \
                                            .withColumn("DAY_DT", current_date()) 
        log.info("Data Frame : 'JNR_Product_Agg_Performance' is built")
        

        # Processing Node : Shortcut_To_Product_Performance_Tgt - Final target dataframe
        Shortcut_To_Product_Performance_Tgt = JNR_Product_Agg_Performance  \
                                                    .select(
                                                            col("DAY_DT"),
                                                            col("PRODUCT_ID"),
                                                            col("PRODUCT_NAME"),
                                                            col("TOTAL_SALES_AMOUNT"),
                                                            col("TOTAL_QUANTITY_SOLD"),
                                                            col("AVG_SALE_PRICE"),
                                                            col("STOCK_QUANTITY"),
                                                            col("REORDER_LEVEL"),
                                                            col("STOCK_LEVEL_STATUS"),
                                                            col("PROFIT"),
                                                            col("CATEGORY")
                                                    )                                                                           
        log.info("Final Data Frame : 'Shortcut_To_Product_Performance_Tgt' is built")
                                             
        # Check for duplicates before load
        checker = Duplicate_check()
        checker.has_duplicates(Shortcut_To_Product_Performance_Tgt, ["PRODUCT_ID", "DAY_DT"])
        
        # Load to PostgreSQL
        load_to_postgres(Shortcut_To_Product_Performance_Tgt, "legacy.product_performance", "append")

    except Exception as e:
        log.error(f"Product Performance processing failed: {str(e)}", exc_info=True)
        raise AirflowException("product_performance ETL failed")

    finally:
        end_session(spark)
