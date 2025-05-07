from pyspark.sql import SparkSession
from airflow.exceptions import AirflowException
import requests
import logging
from secret_key import POSTGRES_PASSWORD

log = logging.getLogger("etl_logger")
log.setLevel(logging.INFO)


def create_session():
    spark = SparkSession.builder \
        .appName("ETL") \
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    return spark

class Extractor:
    def __init__(self, url, token=None):
        self.url = url
        self.token = token

    def extract_data(self):
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        response = requests.get(self.url, headers=headers)

        if response.status_code == 200:
            return response.json().get("data", [])
        else:
            error_msg = f"Failed to fetch from {self.url}. Status: {response.status_code}"
            if response.text:
                error_msg += f" | Response: {response.text[:200]}"
            raise AirflowException(error_msg)

def transform_data(spark, data, key_columns):
    df = spark.createDataFrame(data)

    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.upper())

    duplicate_count = df.count() - df.dropDuplicates(key_columns).count()
    if duplicate_count > 0:
        raise AirflowException(f"Found {duplicate_count} duplicate records based on {key_columns}. ETL process aborted.")

    df_cleaned = df.dropDuplicates(key_columns)
    return df_cleaned

def load_to_postgres(df, table_name):
    jdbc_url = "jdbc:postgresql://host.docker.internal:5432/meta_morph"
    properties = {
        "user": "postgres",
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite",
        properties=properties
    )