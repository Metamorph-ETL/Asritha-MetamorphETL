from utils import create_session
from Raptor.Raptor import Raptor
from airflow.decorators import task
from dags.secret_key import POSTGRES_USER,POSTGRES_PASSWORD 

@task()
def raptor_call():
    spark = create_session()
    raptor = Raptor(spark, POSTGRES_USER, POSTGRES_PASSWORD)
    raptor.submit_raptor_request(
                                 source_type = "pg_admin",
                                 target_type = "pg_admin",
                                 source_db = "meta_morph",
                                 target_db = "meta_morph",
                                 source_sql = "select * from raw.suppliers",
                                 target_sql = "select * from raw.supplier_test",
                                 email = "asritha.vig2338@gmail.com",
                                 output_table_name = "suppliers vs supplier_test",
                                 primary_key = "SUPPLIER_ID"
                                )
    return "Success"

    




