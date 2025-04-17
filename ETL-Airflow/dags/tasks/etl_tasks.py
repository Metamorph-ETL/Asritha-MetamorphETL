from airflow.decorators import task
from airflow.exceptions import AirflowException
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging
from dags.secret_key import POSTGRES_PASSWORD

@task
def m_load_data():
    log = logging.getLogger(__name__)

    try:
        # Initialize Spark session
        log.info("Initializing Spark session")
        spark = SparkSession.builder \
            .appName("USA_Population_ETL") \
            .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.3.jar") \
            .config("spark.master", "local[4]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("INFO")

        #  Extraction
        url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch data: {response.status_code}")
        json_data = response.json().get("data", [])
        log.info("Data fetched successfully")

        #  Define schema using StructType
        schema = StructType([
            StructField("ID Nation", StringType(), True),
            StructField("Nation", StringType(), True),
            StructField("ID Year", IntegerType(), True),
            StructField("Year", StringType(), True),
            StructField("Population", IntegerType(), True),
            StructField("Slug Nation", StringType(), True)
        ])

        # Create DataFrame with schema
        df_raw = spark.createDataFrame(json_data, schema=schema)

        # Step 4: Transformation
        SQ_Shortcut_To_population_data = df_raw.selectExpr(
            "`ID Nation` as ID_NATION",
            "`Nation` as NATION",
            "`ID Year` as ID_YEAR",
            "`Year` as YEAR",
            "`Population` as POPULATION",
            "`Slug Nation` as SLUG_NATION"
        ).dropDuplicates()

        log.info("Data transformed")

        # Step 5: Load to PostgreSQL
        jdbc_url = "jdbc:postgresql://host.docker.internal:5432/data_usa"
        properties = {
            "user": "postgres",
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

        df_transformed.write.jdbc(
            url=jdbc_url,
            table="population_data",
            mode="overwrite",
            properties=properties
        )

        log.info("Data loaded into PostgreSQL successfully")

    except Exception as e:
        raise AirflowException(f"ETL failed: {str(e)}")

    finally:
        if spark:
            spark.stop()
            log.info("Spark session stopped")
