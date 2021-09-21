from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator


from datetime import datetime

args = {

}

with DAG('parallel_task', start_date=datetime(2021,1,1), schedule_interval=None, catchup=False) as dag: 
    
    processing = DummyOperator(task_id='processing')

    cleaning = BashOperator(task_id='cleaning', bash_command='sleep 3')


    processing >> cleaning

