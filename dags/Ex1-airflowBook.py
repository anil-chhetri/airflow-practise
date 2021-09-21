import airflow
from airflow.models import DAG
from airflow.operator.bash import BashOperator
from airflow.operator.bash import PythonOperator

def _get_pictures():
    pass




dag = DAG(
    dag_id="download_rocket_launches",
    start_date= airflow.utils.dates.day_ago(2),
    schedule_interval = None
)


download_launches = BashOperator(
    task_id='Download_Launches',
    command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag
)


get_pictures = PythonOperator(
    task_id = 'get_pictures',
    python_callable= _get_pictures
)


notify = BashOperator(
    task_id='notify',
    command='echo "There are now $(ls /tmp/images/ | wc -l) images."'
)

download_launches >> get_pictures >> notify
