from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime


def func1(execution_date):
    if execution_date.day == 2:
        raise ValueError("Error Occured.")
    print('executing.')


dag = DAG(dag_id="wait_downstream", start_date=datetime(
    2022, 7, 1), schedule_interval='@daily', catchup=True, tags=['mark', 'tutorials'])

with dag as d:

    """
        for every dag run, wait_for_downstream will wait from the task that immediately 
        following the current task to be completed or skipped. from it's previous run.
    """
    

    task_a = BashOperator(
        # using wait_for_downstream will automatically set depends_on_past to true.
        task_id="task_a", bash_command="echo 'task a' && sleep 10", wait_for_downstream=True
    )

    task_b = BashOperator(
        task_id="task_b",
        retries=2,
        bash_command="echo '{{ ti.try_number }}' && exit 0"
    )

    task_c = PythonOperator(
        task_id="task_c",
        python_callable=func1,
        depends_on_past=True
    )

    task_a >> task_b >> task_c
