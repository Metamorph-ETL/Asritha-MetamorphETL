from airflow.decorators import task
from airflow.exceptions import AirflowException
import requests
from pyspark.sql import SparkSession
import logging
from dags.secret_key import POSTGRES_PASSWORD

@task
def m_load_usa():
    log = logging.getLogger(__name__)
    try:
        # Initialize Spark session
        log.info("Initializing Spark session")
        spark = SparkSession.builder \
            .appName("USA_Population_ETL") \
            .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("INFO")
        
        # Extraction: Fetch data from API
        url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch data: {response.status_code}")
        json_data = response.json().get("data", [])
        log.info(f"Extracted {len(json_data)} records")
        
        # Transformation: Convert JSON data to Spark DataFrame and rename columns
        SQ_Shortcut_To_population_data = spark.createDataFrame(json_data)
        Shortcut_To_population_data_tgt = SQ_Shortcut_To_population_data.selectExpr(
            "`ID Nation` as ID_NATION",
            "`Nation` as NATION",
            "`ID Year` as ID_YEAR",
            "`Year` as YEAR",
            "`Population` as POPULATION",
            "`Slug Nation` as SLUG_NATION"
        )

        log.info("Data transformed successfully")
        
          # Load to PostgreSQL
        jdbc_url = "jdbc:postgresql://host.docker.internal:5432/data_usa"
        properties = {
            "user": "postgres",
            "password":POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

#It stores data in public schema
        SQ_Shortcut_To_population_data.write.jdbc(
                url=jdbc_url,
                table="population_data",
                mode="overwrite",
                properties=properties
            )
        log.info("Data written to PostgreSQL successfully.")
        
        
    except Exception as e:
        raise AirflowException(f"ETL failed: {str(e)}")
    finally:
            spark.stop()
            log.info("Spark session stopped")


