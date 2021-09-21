import airflow
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _get_pictures():
    pass




dag = DAG(
    dag_id="download_rocket_launches",
    start_date= airflow.utils.dates.days_ago(2),
    schedule_interval = None,
    tags=['book']
)


download_launches = BashOperator(
    task_id='Download_Launches',
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag
)


get_pictures = PythonOperator(
    task_id = 'get_pictures',
    python_callable= _get_pictures
)


notify = BashOperator(
    task_id='notify',
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."'
)

download_launches >> get_pictures >> notify
