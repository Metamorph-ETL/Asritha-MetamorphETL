from airflow.decorators import task
from datetime import datetime
from Raptor.Raptor import Raptor
from dags.secret_key import POSTGRES_USER,POSTGRES_PASSWORD 
from utils import create_session

@task()
def raptor_call():
        spark = create_session()
        raptor = Raptor(spark, POSTGRES_USER, POSTGRES_PASSWORD )
        raptor.wish("Asritha")
        return "Success"





