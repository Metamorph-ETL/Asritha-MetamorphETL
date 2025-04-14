from pyspark.sql import SparkSession
import requests
import pandas as pd

# Create Spark session
spark = SparkSession.builder \
    .appName("DataUSA_ETL") \
    .config("spark.jars", "file:///C:/spark/jars/postgresql-42.6.0.jar")\
    .getOrCreate()

# Logging
spark.sparkContext.setLogLevel("ERROR")

# Step 1: Extract
url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
raw_data = requests.get(url).json()["data"]

# Step 2: Transform using Pandas first
df = pd.DataFrame(raw_data)
df.rename(columns={
    "ID Nation": "id_nation",
    "Nation": "nation",
    "ID Year": "id_year",
    "Year": "year",
    "Population": "population",
    "Slug Nation": "slug_nation"
}, inplace=True)

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Step 3: Load to PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/data_usa"
properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

spark_df.write.jdbc(url=jdbc_url, table="population_data", mode="overwrite", properties=properties)
