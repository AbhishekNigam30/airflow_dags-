from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from airflow.operators.bash import BashOperator

default_args = {'owner': 'airflow',
                'start_date': datetime(2021, 1, 1)
                }
dag = DAG ( dag_id,
            schedule_interval=scheduletoreplace,
            default_args=default_args,
            catchup=False)

with dag:
    t1 = BashOperator(
        task_id="notebook", 
        bash_command='echo "dynamic-dagrun"'
        )