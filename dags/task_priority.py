"""
    task priority specifies the order in which parallel task can execute.
    task priority are compared on the based of the pools in which the following task are executing.
    for eg:
        if taskA has priority 6 in pool A.
        and taskB has priority 7 in pool B
        then both of these task will execute at same time since both the task belongs to
        two different pools.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.helpers import cross_downstream

from datetime import datetime

dag = DAG(
    dag_id="pirority_weight",
    schedule_interval='@daily',
    start_date=datetime(2022, 7, 10),
    catchup=False,
    tags=['mark']
)

with dag as d:

    extracct_a = BashOperator(
        task_id="extract_a",
        bash_command="echo '{{ ti.priority_weight }}'"
    )

    extract_b = BashOperator(
        task_id="extract_b",
        bash_command="echo '{{ ti.priority_weight }}'"
    )

    process_a = BashOperator(
        task_id="process_a",
        bash_command="echo '{{ ti.priority_weight }}' && sleep 10",
        pool='Experiment_pool',
        priority_weight=3
    )

    process_b = BashOperator(
        task_id="process_b",
        bash_command="echo '{{ ti.priority_weight }}' && sleep 10",
        pool='Experiment_pool',
        priority_weight=1
    )

    process_c = BashOperator(
        task_id="process_c",
        bash_command="echo '{{ ti.priority_weight }}' && sleep 10",
        pool='Experiment_pool',
        priority_weight=5
    )

    store = BashOperator(
        task_id='store',
        bash_command="echo 'completed.'"
    )

    cross_downstream([extracct_a, extract_b], [
                     process_a, process_b, process_c])
    [process_a, process_b, process_c] >> store
