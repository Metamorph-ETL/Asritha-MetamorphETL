from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadToPostgres") \
    .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

df = spark.read.csv("/opt/spark-data/transformed_data.csv", header=True, inferSchema=True)

jdbc_url = "jdbc:postgresql://host.docker.internal:5432/data_usa"
properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

try:
    df.write.jdbc(url=jdbc_url, table="population_data", mode="overwrite", properties=properties)
    print(" Data loaded to PostgreSQL")
except Exception as e:
    print(" Load failed:", e)
