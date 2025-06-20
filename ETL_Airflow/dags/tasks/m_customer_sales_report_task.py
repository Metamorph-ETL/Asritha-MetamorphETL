from airflow.decorators import task
from utils import create_session, load_to_postgres, end_session, read_from_postgres, log, Duplicate_check
from pyspark.sql.functions import col, current_date, date_sub, month, year, percent_rank, when, row_number, current_timestamp, round
from pyspark.sql.window import Window
from airflow.exceptions import AirflowException

@task(task_id="m_load_customer_sales_report")
def m_load_customer_sales_report():
    try:
        spark = create_session()
        
        # Processing Node : SQ_Shortcut_To_Sales - Reads data from 'raw.sales' table
        SQ_Shortcut_To_Sales = read_from_postgres(spark, "raw.sales") \
                                    .select(
                                        col("SALE_ID"),
                                        col("SALE_DATE"),
                                        col("QUANTITY"),
                                        col("CUSTOMER_ID"),
                                        col("PRODUCT_ID"),
                                        col("ORDER_STATUS"),
                                        col("DISCOUNT")
                                    )
        log.info("Data Frame : 'SQ_Shortcut_To_Sales' is built")
        
        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products") \
                                        .select(
                                            col("PRODUCT_ID"),                         
                                            col("PRODUCT_NAME"),
                                            col("CATEGORY"),
                                            col("SELLING_PRICE")
                                        )
        log.info("Data Frame : 'SQ_Shortcut_To_Products' is built")

        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.customers' table
        SQ_Shortcut_To_Customers = read_from_postgres(spark, "raw.customers") \
                                        .select(
                                            col("CUSTOMER_ID"),
                                            col("NAME"),
                                            col("CITY")                                
                                        )
        log.info("Data Frame : 'SQ_Shortcut_To_Customers' is built")

        # Processing Node : FIL_Sales_Data - Filter out records where ORDER_STATUS is 'Delivered' and 'Shipped'
        FIL_Sales_Data = SQ_Shortcut_To_Sales \
                                  .filter(
                                       col("ORDER_STATUS").isin("Delivered", "Shipped")
                                   )
        log.info("Data Frame : 'FIL_Sales' is built") 

        # Processing Node : JNR_Sales_Products - Joins data from 'FIL_Sales_Data' and 'SQ_Shortcut_To_Products' dataframes
        JNR_Sales_Products = FIL_Sales_Data \
                                 .join(
                                        SQ_Shortcut_To_Products,
                                        on="PRODUCT_ID",
                                        how="left"
                                 ) \
                                 .select(
                                        col("SALE_ID"),
                                        col("SALE_DATE"),
                                        col("QUANTITY"),
                                        col("SELLING_PRICE"),
                                        col("CUSTOMER_ID"),
                                        col("PRODUCT_ID"),
                                        col("ORDER_STATUS"),
                                        col("DISCOUNT"),
                                        col("PRODUCT_NAME"),
                                        col("CATEGORY")
                                 )
        log.info("Data Frame : 'JNR_Sales_Products' is built") 

        # Processing Node : JNR_All_Entities - Joins data from JNR_Sales_Products and SQ_Shortcut_To_Customers dataframes
        JNR_All_Entities = JNR_Sales_Products \
                                    .join(
                                         SQ_Shortcut_To_Customers,
                                         on="CUSTOMER_ID",
                                         how="inner"                                     
                                    ) \
                                    .select(
                                        col("SALE_ID"),
                                        col("SALE_DATE"),
                                        col("QUANTITY"),
                                        col("SELLING_PRICE"),
                                        col("CUSTOMER_ID"),
                                        col("PRODUCT_ID"),
                                        col("DISCOUNT"),
                                        col("PRODUCT_NAME"),
                                        col("CATEGORY"),
                                        col("NAME").alias("CUSTOMER_NAME"),
                                        col("CITY")
                                    )
        log.info("Data Frame : 'JNR_All_Entities' is built") 

        # Processing Node : EXP_Calculate_Metrics - Perform core metric calculations such as SALE_DATE, SALE_AMOUNT and add audit columns
        EXP_Calculate_Metrics = JNR_All_Entities \
                                    .withColumn("DAY_DT", current_date()) \
                                    .withColumn("SALE_DATE", date_sub(current_date(), 1)) \
                                    .withColumn("SALE_MONTH", month(col("SALE_DATE"))) \
                                    .withColumn("SALE_YEAR", year(col("SALE_DATE"))) \
                                    .withColumn("PRICE", round(col("SELLING_PRICE"), 2)) \
                                    .withColumn("SALE_AMOUNT", round(col("QUANTITY") * col("SELLING_PRICE") * (1 - col("DISCOUNT")/100), 2)) \
                                    .withColumn("LOAD_TSTMP", current_timestamp())
        log.info(f"Data Frame : 'EXP_Calculate_Metrics' is built")
        
        # Define a window to rank all rows globally by SALE_AMOUNT in descending order (highest sales first)
        window_spec = Window.orderBy(col("SALE_AMOUNT").desc())
        
        # Processing Node: EXP_Loyalty_Tier_Classification - Apply percent_rank window function to segment customers by their spending
        EXP_Loyalty_Tier_Classification = EXP_Calculate_Metrics \
                                                .withColumn("percent_rank", percent_rank()
                                                .over(window_spec)) \
                                                .withColumn("LOYALTY_TIER",
                                                        when(
                                                            col("percent_rank") <= 0.2, 
                                                            "Gold"
                                                        ) 
                                                        .when(
                                                            (col("percent_rank") > 0.2) & (col("percent_rank") <= 0.5),
                                                            "Silver"
                                                        )
                                                        .otherwise("Bronze")
                                                )
        log.info("Data Frame: 'EXP_Loyalty_Tier_Classification' is built")

        # Defined a window to rank products per customer by descending "SALE_AMOUNT"
        product_rank_window = Window.partitionBy("CUSTOMER_ID").orderBy(col("SALE_AMOUNT").desc())
        
        # Processing Node: EXP_Flag_Top_Performers - Apply row_number over product_rank_window to identify top-selling product per customer
        EXP_Flag_Top_Performers = EXP_Loyalty_Tier_Classification \
                                        .withColumn("rn", row_number().over(product_rank_window)) \
                                        .withColumn("TOP_PERFORMER",
                                                     when(
                                                         col("rn") == 1,
                                                         "Yes"
                                                     )
                                                     .otherwise("No")
                                        ) \
                                        .drop("rn", "percent_rank")
        log.info("Data Frame: 'EXP_Flag_Top_Performers' is built")
        
        # Processing Node : Shortcut_To_Customer_Sales_Report_tgt - Final target dataframe
        Shortcut_To_Customer_Sales_Report_tgt = EXP_Flag_Top_Performers \
                                                    .select(
                                                        col("DAY_DT"),
                                                        col("CUSTOMER_ID"),
                                                        col("CUSTOMER_NAME"),
                                                        col("SALE_ID"),
                                                        col("CITY"),
                                                        col("PRODUCT_NAME"),
                                                        col("CATEGORY"),
                                                        col("SALE_DATE"),
                                                        col("SALE_MONTH"),
                                                        col("SALE_YEAR"),
                                                        col("QUANTITY"),
                                                        col("PRICE"),
                                                        col("SALE_AMOUNT"),
                                                        col("TOP_PERFORMER"),
                                                        col("LOYALTY_TIER"),
                                                        col("LOAD_TSTMP") 
                                                    )
        # Check for duplicates before load
        checker = Duplicate_check()
        checker.has_duplicates(Shortcut_To_Customer_Sales_Report_tgt, ["SALE_ID", "DAY_DT"])

        # Load to PostgreSQL
        load_to_postgres(Shortcut_To_Customer_Sales_Report_tgt, "legacy.customer_sales_report", "append")

        return "Task for loading Customer Sales Report got completed Successfully"

    except Exception as e:
        log.error(f"Customer Sales Report processing failed: {str(e)}", exc_info=True)
        raise AirflowException("Customer Sales Report ETL failed")

    finally:
        end_session(spark)

                                            
        
        