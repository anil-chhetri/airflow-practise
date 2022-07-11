from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator

from airflow.utils.helpers import cross_downstream


from datetime import datetime


def func(execution_date):
    print('python callable.')
    if execution_date.day == 6:
        raise ValueError('python callable produce error')


dag = DAG(dag_id="poolexperiment", start_date=datetime(
    2022, 7, 1), schedule_interval='@daily', catchup=False, tags=['mark'])

with dag as d:

    extract_a = BashOperator(
        task_id="extract_a",
        bash_command="echo '{{ti.try_number}}'"
    )

    extract_b = BashOperator(
        task_id="extract_b",
        bash_command="echo '{{ti.try_number}}' && echo 'new operator'"
    )

    process_a = BashOperator(
        task_id='process_a',
        bash_command="echo 'process a' &&  sleep 20",
        pool="Experiment_pool"

    )

    process_b = BashOperator(
        task_id='process_b',
        bash_command="echo 'process a' &&  sleep 20",
        pool="Experiment_pool"
    )

    process_c = BashOperator(
        task_id='process_c',
        bash_command="echo 'process a' &&  sleep 20",
        pool="Experiment_pool"
    )

    store = PythonOperator(
        task_id="store",
        python_callable=func,
        depends_on_past=True
    )

    # dummy = DummyOperator(
    #     task_id="dummy"
    # )

    cross_downstream([extract_a, extract_b], [process_a, process_b, process_c])
    [process_a, process_b, process_c] >> store
    # extract_a >> process_a
