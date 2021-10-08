import airflow.utils.dates as date

from airflow.models import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='Python_templating',
    start_date= date.days_ago(2),
    schedule_interval=None
)

#https://dumps.wikimedia.org/other/pageviews/2021/2021-10/pageviews-20211008-020000.gz

def _get_data(**kwargs):
    print('from python callable')
    print(kwargs.get('execution_date'))
    year, month, day, hour, *_ = kwargs.get('execution_date').timetuple()
    print(year, month, day, hour)
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month}/pageviews-"
        f"{year}"

    )


get_data = PythonOperator(
    task_id = 'get_data',
    python_callable=_get_data,
    dag=dag
)