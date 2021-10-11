from airflow.models import DAG

import airflow.utils.dates as d
from demo_plugins import DataTransferOperator, FileSensorCount



with DAG(
    dag_id='data_transfer_custom',
    start_date=d.days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['custom plugins operator', 'data transfer']
) as dag:

    t1 = DataTransferOperator(
        task_id = 'transfer',
        source_file_path = '/home/anil/del/test/test',
        destination_file_path = '/home/anil/del/test/test_d',
        delete_words=['airflow', 'plugins']
    )


    s1 = FileSensorCount(
        task_id = 'fileSensor',
        file_conn = 'fs_default',
        file_path ='/home/anil/del/test',
        poke_interval = 5,
        timeout =100
    )