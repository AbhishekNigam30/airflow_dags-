from airflow import DAG, XComArg
try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json
from airflow.decorators import task
from airflow.models import Variable
from airflow.models import TaskInstance
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

default_args = {
    'start_date': datetime.now(),
    'azure_data_factory_conn_id': 'Azure_datafactory_conn',
    'factory_name' : "AzureDataFact0425",
    'resource_group_name':'Azure_Training'
}



def get_filename_func(ti, **kwargs):
    """
    Function is responsible for retrieving the file names passed to the paramters 
    and store it inside the xcom for later use
    """
    print("CUSTOM PRINTING STATEMENT")
    dag_run = kwargs.get('dag_run')
    print(dag_run)
    file_n = dag_run.conf.get('filename')
    print(file_n)
    with open("D:/airflow-docker-local/airflow-docker-local/include/dag-template1.py") as f2:
        with open("D:/airflow-docker-local/airflow-docker-local/dags/"+file_n+".py", "w") as f1:
            for line in f2:
                f1.write(line)
    # with open("D:/airflow-docker-local/airflow-docker-local/dags/"+config['DagId']+".py", "w") as f3:
    #         for line in f3:
    #             if line.__contains__("dag_id"):
    #                 line.replace("dag_id", "'"+config['DagId']+"'")
    #             elif line.__contains__("scheduletoreplace"):
    #                 line.replace("scheduletoreplace", config['Schedule'])

    for line in fileinput.input("D:/airflow-docker-local/airflow-docker-local/dags/"+file_n+".py", inplace=True):
        if line.__contains__("dag_id"):
            line=line.replace("dag_id", "'"+file_n+"'")
        elif line.__contains__("filename"):    
            line=line.replace("scheduletoreplace", file_n)
        print(line,end="")

with DAG('apifetch', default_args=default_args, catchup=False) as dag:

    # Begin and End empty operator
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    # Task group to get and process the incoming files
    # with TaskGroup('Process_Filename') as process_filename:
    # print(XCom Args)
    set_filename = PythonOperator(
        task_id='Set_Uploaded_Files',
        python_callable=get_filename_func,
        provide_context=True,
        do_xcom_push=True,
    )    
begin>>set_filename>>end