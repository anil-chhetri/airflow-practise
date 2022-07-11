from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    "owner": "anil"
}


def func1(execution_date):
    print(execution_date)
    if execution_date.day == 3:
        raise ValueError("error day occured.")


dag = DAG(dag_id="depends_on_past", default_args=default_args, start_date=datetime(
    2022, 7, 1), schedule_interval="@daily", dagrun_timeout=40  # this parameter is good to have if we have set depends_on_past flag to true for task.
    , catchup=True, tags=["airlfow", "mark"])

with dag as d:

    task_a = BashOperator(
        task_id="task_a",
        bash_command="echo 'task_a' && sleep 10"
    )

    # task_b = BashOperator(
    #     task_id="task_b",
    #     retries=3,  # will retry this task for 4 times.
    #     retry_exponential_backoff=True,
    #     retry_delay=timedelta(seconds=10),
    #     bash_command="echo '{{ ti.try_number }}' && exit 1"
    # )

    task_b = BashOperator(
        task_id="task_b_0_1",
        retries=3,  # will retry this task for 4 times.
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=10),
        bash_command="echo '{{ ti.try_number }}' && exit 0"
    )

    task_c = PythonOperator(
        task_id="task_c",
        python_callable=func1,
        depends_on_past=True

    )

    task_a >> task_b >> task_c
