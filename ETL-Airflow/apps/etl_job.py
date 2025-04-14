from pyspark.sql import SparkSession
import requests
import pandas as pd
import psycopg2
import traceback

def run_etl():
    # Step 1: Create Spark session with JDBC jar
    print("welcome")
    """spark = SparkSession.builder \
        .appName("DataUSA_ETL") \
        .getOrCreate()

    #spark.sparkContext.setLogLevel("INFO")"""
    print("hlooooo")

    # Step 2: Extract data from REST API
    url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
    raw_data = requests.get(url).json()["data"]

    # Step 3: Transform using pandas
    df = pd.DataFrame(raw_data)
    print(df)

    # Rename columns: space to underscore, capital to small
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    # Show row count and sample
    print("Row count:", spark_df.count())
    spark_df.show()

    # Step 4: Load into PostgreSQL
    jdbc_url = "jdbc:postgresql://host.docker.internal:5432/data_usa"
    properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    try:
        spark_df.write.jdbc(
            url=jdbc_url,
            table="population_data",
            mode="overwrite",
            properties=properties
        )
        print("Data written to PostgreSQL successfully!")
    except Exception as e:
        print("Failed to write to PostgreSQL:")
        traceback.print_exc()

