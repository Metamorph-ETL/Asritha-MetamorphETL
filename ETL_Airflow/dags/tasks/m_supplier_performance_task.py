from airflow.decorators import task
from utils import create_session, load_to_postgres, Duplicate_check, end_session, log, read_from_postgres
from pyspark.sql.functions import sum, col, count, current_date, when, lit, row_number, desc, coalesce, trim, round, upper
from pyspark.sql.window import Window
from airflow.exceptions import AirflowException

@task(task_id="m_load_suppliers_performance")
def m_load_suppliers_performance():
    try:
        spark = create_session()

        # Processing Node : SQ_Shortcut_To_Sales - Reads data from 'raw.sales_pre' table
        SQ_Shortcut_To_Sales = read_from_postgres(spark, "raw.sales_pre") \
                                            .select(
                                                col("SALE_ID"),
                                                col("PRODUCT_ID"),
                                                col("QUANTITY"),
                                                col("ORDER_STATUS"),
                                                col("DISCOUNT")
                                            )
        log.info("Data Frame : 'SQ_Shortcut_To_Sales' is built")

        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products_pre' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products_pre") \
                                        .select(
                                                col("PRODUCT_ID"),
                                                col("SUPPLIER_ID"),
                                                col("PRODUCT_NAME"),
                                                col("SELLING_PRICE")
                                            )
        log.info("Data Frame : 'SQ_Shortcut_To_Products' is built")

        # Processing Node : SQ_Shortcut_To_Suppliers - Reads data from 'raw.suppliers_pre' table
        SQ_Shortcut_To_Suppliers = read_from_postgres(spark, "raw.suppliers_pre") \
                                       .select(
                                            col("SUPPLIER_ID"),
                                            col("SUPPLIER_NAME")
                                        )
        log.info("Data Frame : 'SQ_Shortcut_To_Suppliers' is built")

        # Processing Node : FIL_Sales_Cancelled - Filters out cancelled sales
        FIL_Sales_Cancelled = SQ_Shortcut_To_Sales \
                                      .filter(
                                          col("ORDER_STATUS") != "Cancelled"
                                      )
        log.info("Data Frame : 'FIL_Sales_Cancelled' is built")

        # Processing Node : JNR_Products_Suppliers - Join products with suppliers (right join to keep all suppliers)
        JNR_Products_Suppliers = SQ_Shortcut_To_Products \
                                            .join(
                                                SQ_Shortcut_To_Suppliers, 
                                                on="SUPPLIER_ID", 
                                                how="right"
                                            ) \
                                            .select(
                                                SQ_Shortcut_To_Products.PRODUCT_ID,
                                                SQ_Shortcut_To_Products.SUPPLIER_ID,
                                                SQ_Shortcut_To_Products.PRODUCT_NAME,
                                                SQ_Shortcut_To_Products.SELLING_PRICE,
                                                SQ_Shortcut_To_Suppliers.SUPPLIER_NAME
                                            )
        log.info("Data Frame : 'JNR_Products_Suppliers' is built")

        # Processing Node : JNR_Sales_Products_Suppliers - Join filtered sales with the above joined data
        JNR_Sales_Products_Suppliers = JNR_Products_Suppliers \
                                                .join(FIL_Sales_Cancelled, 
                                                      on="PRODUCT_ID", 
                                                      how="left"
                                                ) \
                                                .select(
                                                    "SALE_ID",
                                                    "PRODUCT_ID",
                                                    "PRODUCT_NAME",
                                                    "SUPPLIER_ID",
                                                    "SUPPLIER_NAME",
                                                    "SELLING_PRICE",
                                                    "QUANTITY",
                                                    "DISCOUNT"
                                                ) \
                                                .withColumn("QUANTITY", when(col("QUANTITY").isNull(), lit(0)).otherwise(col("QUANTITY"))) \
                                                .withColumn("DISCOUNT", when(col("DISCOUNT").isNull(), lit(0)).otherwise(col("DISCOUNT"))) \
                                                .withColumn("REVENUE", (coalesce(col("SELLING_PRICE"), lit(0)) * (1 - coalesce(col("DISCOUNT"), lit(0)) / 100)) * coalesce(col("QUANTITY"), lit(0)))
        log.info("Data Frame : 'JNR_Sales_Products_Suppliers' is built")

        # Processing Node : AGG_Supplier_Product - Aggregate revenue and quantity at supplier-product level
        AGG_Supplier_Product = JNR_Sales_Products_Suppliers \
                                        .groupBy(
                                            "SUPPLIER_ID",
                                            "SUPPLIER_NAME",
                                            "PRODUCT_ID",
                                            "PRODUCT_NAME"
                                        ) \
                                        .agg(
                                            count("SALE_ID").alias("agg_product_sales_count"),
                                            sum("REVENUE").alias("agg_REVENUE"),
                                            sum("QUANTITY").alias("agg_QUANTITY")
                                        )
        log.info("Data Frame : 'AGG_Supplier_Product' is built")
        
        #Window to rank products per supplier by revenue and product name 
        window_spec = Window.partitionBy("SUPPLIER_ID").orderBy(
                            col("agg_REVENUE").desc(),
                            trim(upper(col("PRODUCT_NAME"))).desc()
                        )

        # Processing Node : FIL_Top_Products - Get top-selling product per supplier
        FIL_Top_Products = AGG_Supplier_Product \
                                    .withColumn("rank", row_number().over(window_spec)) \
                                    .filter(col("rank") == 1) \
                                    .select(
                                        col("SUPPLIER_ID"),
                                        col("PRODUCT_NAME").alias("TOP_SELLING_PRODUCT")
                                    )
        log.info("Data Frame : 'FIL_Top_Products' is built")

        # Processing Node : AGG_Supplier - Aggregate metrics at supplier level
        AGG_Supplier = AGG_Supplier_Product \
                                .groupBy("SUPPLIER_ID", "SUPPLIER_NAME") \
                                .agg(
                                    sum("agg_REVENUE").alias("TOTAL_REVENUE"),
                                    sum("agg_QUANTITY").alias("TOTAL_STOCK_SOLD"),
                                    sum("agg_product_sales_count").alias("TOTAL_PRODUCTS_SOLD")  
                                )
        log.info("Data Frame : 'AGG_Supplier' is built")

        # Processing Node : JNR_Supplier_With_Aggs - Join all suppliers with aggregate metrics
        JNR_Supplier_With_Aggs = SQ_Shortcut_To_Suppliers \
                                     .join(
                                         AGG_Supplier, 
                                         on=["SUPPLIER_ID", "SUPPLIER_NAME"],
                                         how="left"
                                     )
        log.info("Data Frame : 'JNR_Supplier_With_Aggs' is built")

        # Processing Node : JNR_AGG_Supplier_Product_TOP_Product - Final join with top product
        JNR_AGG_Supplier_Product_TOP_Product = JNR_Supplier_With_Aggs \
                                                        .join(
                                                            FIL_Top_Products,
                                                            on="SUPPLIER_ID", 
                                                            how="left"
                                                        ) \
                                                        .withColumn("DAY_DT", current_date()) \
                                                        .withColumn("SUPPLIER_ID", trim(col("SUPPLIER_ID"))) \
                                                        .withColumn("SUPPLIER_NAME", trim(col("SUPPLIER_NAME"))) \
                                                        .withColumn("TOTAL_REVENUE", round(coalesce(col("TOTAL_REVENUE"), lit(0)), 2)) \
                                                        .withColumn("TOTAL_PRODUCTS_SOLD", coalesce(col("TOTAL_PRODUCTS_SOLD"), lit(0))) \
                                                        .withColumn("TOTAL_STOCK_SOLD", coalesce(col("TOTAL_STOCK_SOLD"), lit(0))) \
                                                        .withColumn("TOP_SELLING_PRODUCT", trim(coalesce(col("TOP_SELLING_PRODUCT"), lit(""))))\
                                                        .orderBy(col("TOTAL_REVENUE").desc())
        log.info("Data Frame : 'JNR_AGG_Supplier_Product_TOP_Product' is built")

        # Processing Node : Shortcut_To_Supplier_Performance_Tgt - Final target DataFrame
        Shortcut_To_Supplier_Performance_Tgt = JNR_AGG_Supplier_Product_TOP_Product \
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

        # Check for duplicates before loading
        checker = Duplicate_check()
        checker.has_duplicates(Shortcut_To_Supplier_Performance_Tgt, ["SUPPLIER_ID", "DAY_DT"])

        # Load to target table
        load_to_postgres(Shortcut_To_Supplier_Performance_Tgt, "legacy.supplier_performance", "append")

        return "Supplier performance task completed successfully."

    except Exception as e:
        log.error(f"Supplier performance calculation failed: {str(e)}", exc_info=True)
        raise AirflowException("Supplier_performance ETL failed")

    finally:
        end_session(spark)