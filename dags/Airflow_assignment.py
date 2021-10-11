import  airflow.utils.dates as d

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from pathlib import Path

#Build a DAG that will create a new directory (let say 'test_dir') inside the dags folder, and crosscheck the results at task level.

with DAG(
    dag_id='assignment',
    start_date=d.days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assignment', 'udemy A-Z']
) as d:

    create_dir = BashOperator(
        task_id = 'create_dir',
        bash_command=''' mkdir {{params.path }}/test_dir ''',
        params={"path" : "'/home/anil/airflow practise/airflow-practise/dags'"}
    )

    def _check_dir(folder_path):
        if (not Path(folder_path).exists()):
            raise ValueError('folder not found.')


    check_dir = PythonOperator(
        task_id = 'check_dir',
        python_callable=_check_dir,
        op_args=[f"/home/anil/airflow practise/airflow-practise/dags/test_dir"]
    )

    create_dir >> check_dir


    



