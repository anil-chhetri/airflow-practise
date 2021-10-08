import airflow.utils.dates 

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id='chapter4_templating',
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
    catchup=False,
    tags=['Book', 'BashOperator', 'python operator']
)


# # https://dumps.wikimedia.org/other/pageviews/
# {year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz

# https://dumps.wikimedia.org/other/pageviews/2019/2019-07/pageviews-20190707-110000.gz

#https://dumps.wikimedia.org/other/pageviews/2021/2021-10/pageviews-20211008-020000.gz
#https://dumps.wikimedia.org/other/pageviews/2021/2021-10/pageviews-20211008-020000.gz']


get_data = BashOperator(
    task_id = 'get_data',
    bash_command=(
        "curl -f --output /tmp/wikipageviews.gz "
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year }}/"
        "{{ execution_date.year }}-"
        "{{ '{:02}'.format(execution_date.month) }}/"
        "pageviews-{{ execution_date.year }}"
        "{{ '{:02}'.format(execution_date.month) }}"
        "{{ '{:02}'.format(execution_date.day)  }}-"
        "{{ '{:02}'.format(execution_date.hour-1) }}0000.gz"
    ),
    dag=dag

)






