from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import json

def _read_file():
    with open('dags/sample_file.json') as f:
        data = json.load(f)
        Variable.set(key='list_of_cities',
                     value=data['cities'], serialize_json=True)
        print('Loading Variable from file...'+str(data))

def _say_hello(city_name):
    print('hello from ' + city_name)

with DAG('dynamic_tasks_from_var', schedule_interval='@once',
         start_date=days_ago(2),
         catchup=False) as dag:

    read_file = PythonOperator(
        task_id='read_file',
        python_callable=_read_file
    )
