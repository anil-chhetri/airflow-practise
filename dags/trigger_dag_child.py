from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime

default_args = {
    "start_date": datetime(2020, 8, 9)
}

dag = DAG(dag_id="child_dag", default_args=default_args,
          schedule_interval=None, catchup=False, tags=["triggerdagRun"])

with dag:

    start = DummyOperator(task_id="start")

    process = BashOperator(
        task_id="process",
        bash_command="echo 'process done'"
    )

    getting_parent_value = BashOperator(
        task_id="getting_parenet_value",
        bash_command="echo '{{ dag_run.conf['path'] }}'"
    )

    end = DummyOperator(task_id="end")

    start >> process >> getting_parent_value >> end
