from pyspark.sql import SparkSession
import requests
from secret_key import POSTGRES_PASSWORD
from pyspark.sql.functions import count
import logging
from airflow.exceptions import AirflowException
from secret_key import TOKEN_URL,USERNAME,PASSWORD

# Initialize logger
log = logging.getLogger("etl_logger")
log.setLevel(logging.INFO)

#create and configure Spark session
def create_session():
    log.info("Starting ETL process")
    spark = SparkSession.builder\
        .appName("ETL")\
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    log.info("Spark session initialized")
    return spark

#Extractor class handles API data extraction 
class Extractor:
    def __init__(self, url, token=None):
        self.url = url
        self.token = token

# Automatically fetch token if it's a customer API and no token is provided
        if "customers" in url and token is None:  
            self.token = self._get_token()

# Private method to fetch bearer token from TOKEN_URL
    def _get_token(self):
        try:
            response = requests.post(TOKEN_URL, data={
                "username": USERNAME,
                "password": PASSWORD,  
            })

            if response.status_code == 200:
                token = response.json().get("access_token")
                return token
            else:
                raise AirflowException(f"Failed to fetch token. Status: {response.status_code}")
        except Exception as e:
            log.error(f"Token fetch failed: {str(e)}", exc_info=True)
      
    def extract_data(self):
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
  
        response = requests.get(self.url, headers=headers)
        if response.status_code == 200:
            data = response.json().get("data", [])
            log.info(f"Extracted {len(data)} records from {self.url}")
            return data
        else:
            error_msg = f"Failed to fetch from {self.url}. Status: {response.status_code}"
            log.error(error_msg)
            raise AirflowException(error_msg)



# Custom exception for duplicate detection

class DuplicateException(Exception):
    def __init__(self, message):
        super().__init__(message)

class Duplicate_check:
    @classmethod
    def has_duplicates(cls, df, primary_key_list):
        logging.info("Checking for duplicates in the given data")
        grouped_df = df.groupBy(primary_key_list)\
                      .agg(count('*').alias('cnt'))\
                      .filter('cnt > 1')
        if grouped_df.count() > 0:
            raise DuplicateException(f"Found duplicates in columns: {primary_key_list}")
        logging.info("No duplicates found")


    
# Function to load Spark DataFrame to PostgreSQL table
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
    log.info("Loaded data successfully")