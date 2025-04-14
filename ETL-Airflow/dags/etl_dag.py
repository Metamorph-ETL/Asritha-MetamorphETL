#Importing necessary modules from Airflow, Python standard libraries, and PySpark
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import requests
from airflow.exceptions import AirflowException
import json
import logging  

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,  
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='usa_population_etl_taskflow',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL DAG using TaskFlow API with Spark and PostgreSQL',
    tags=['etl', 'spark', 'clean']
) as dag:

# Define the ETL task using TaskFlow API
    @task()
    def run_etl():
        log = logging.getLogger(__name__)  
        spark = None

        try:
            # Initialize Spark 
            log.info("Initializing Spark session")
            spark = SparkSession.builder \
                  .appName("USA_Population_ETL") \
                  .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.3.jar") \
                  .config("spark.master", "local[4]") \
                  .config("spark.driver.memory", "2g") \
                  .config("spark.executor.memory", "2g") \
                  .config("spark.sql.shuffle.partitions", "8") \
                  .config("spark.network.timeout", "600s") \
                  .getOrCreate() 
            spark.sparkContext.setLogLevel("INFO")
            log.info("Spark initialized successfully")

            # API Extraction
            log.info("Fetching data from API")
            try:
                url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                json_data = response.json().get("data", [])
                if not json_data:
                    raise ValueError("Empty API response")
                log.info(f"Fetched {len(json_data)} records")
            except Exception as e:
                raise RuntimeError(f"API Error: {str(e)}")

            # Spark Transformation
            try:
                df = spark.createDataFrame(json_data)  # Convert JSON to Spark DataFrame
                # Rename columns for easier handling
                renamed_df = df.selectExpr(
                   "`ID Nation` as id_nation",
                   "`Nation` as nation",
                   "`ID Year` as id_year",
                   "`Year` as year",
                   "`Population` as population",
                   "`Slug Nation` as slug_nation"
)

                log.info("Transformed data schema:")
                renamed_df.printSchema()
            except Exception as e:
                raise RuntimeError(f"Transformation failed: {str(e)}")

            # PostgreSQL Load
            try:
                renamed_df.write \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql://host.docker.internal:5432/data_usa") \
                    .option("dbtable", "population_data") \
                    .option("user", "postgres") \
                    .option("password", "postgres") \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("overwrite") \
                    .save()
                    
                log.info(f"Successfully wrote {renamed_df.count()} records")
            except Exception as e:
                raise RuntimeError(f"Database write failed: {str(e)}")

        except Exception as e:
            log.error(f"ETL Pipeline Failed: {str(e)}")
            raise AirflowException("ETL failed")  # Proper Airflow exception
        finally:
            if spark:
                spark.stop()
                log.info("Spark session terminated")
# Trigger the task
    run_etl()
    