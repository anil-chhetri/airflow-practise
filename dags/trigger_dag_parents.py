from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

default_args = {
    "start_date": datetime(2020, 8, 9)
}

dag = DAG(dag_id="parent_dag", default_args=default_args,
          schedule_interval=None, catchup=False, tags=["triggerdagRun"])

with dag:

    start = DummyOperator(task_id="start")

    calling_dag = TriggerDagRunOperator(
        task_id="calling_dag",
        trigger_dag_id="child_dag",
        conf={  # passing addition information to child dag.
            "path": "/opt/airflow/"
        },
        execution_date="{{ ds }}",  # sending execution date to child dag
        # necessary if child dag fail and we need to rerun the dags.
        reset_dag_run=True,
        wait_for_completion=True,  # wait for the dag to completes.
        poke_interval=60,  # set to 60 seconds.
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'all done'"
    )

    start >> calling_dag >> end
