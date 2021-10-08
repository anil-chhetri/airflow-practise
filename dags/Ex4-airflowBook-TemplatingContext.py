import airflow.utils.dates as d

from airflow.models import DAG
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id='Print_context',
    start_date = d.days_ago(2),
    schedule_interval=None,
    tags=['Book', 'Python Operator']    
)


def printContext(**kwargs):
    print('from python function.')
    print(kwargs)
    print('context list ends.')



print_context = PythonOperator(
    task_id = 'print_context',
    python_callable=printContext,
    dag=dag
)

