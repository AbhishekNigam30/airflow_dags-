import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#from airflow.operators.bash_operator import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
# default arguments for all the tasks
default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com']        ,
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0)
}
# create a DAG definition
dag = DAG(
'databricks_demo',
default_args=default_args,
schedule_interval=timedelta(days=1),
description='DAG in charge of DatabricksSubmitRunOperator'
)
# create a simple task that prints todays date
date_task = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)    

# create a cluster config
new_cluster_conf = {
    'spark_version': '10.4.x-scala2.12',
    'node_type_id': 'Standard_DS3_v2',
    'autoscale' : {
        'min_workers': 1,
        'max_workers': 8
    },
    'spark_conf': {
        'spark.databricks.delta.preview.enabled': 'true',
        'spark.sql.crossJoin.enabled': 'true',
        },
        'spark_env_vars': {
            'PYSPARK_PYTHON': '/databricks/python3/bin/python3'
        },
}    

notebook_task_params = {
        'new_cluster': new_cluster_conf,
        'notebook_task': {
        'notebook_path': '/Users/manish.mehta@tigeranalytics.com/demo-workbook',
        }
}
# create a task to run a notebook using above config
notebook_task = DatabricksSubmitRunOperator(
            task_id='notebook_task',
            dag=dag,
            json=notebook_task_params,
            do_xcom_push = True
)


# set the order of tasks
date_task >> notebook_task