
from fileinput import filename
from pathlib import Path

from airflow import DAG
import airflow
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.decorators import task

import IngestData

import os
import shutil

airflow_home = os.environ.get('AIRFLOW_HOME')

default_args = {
    "owner": "anil",
    "retries" : 0, 
    "depends_on_past":  False
}



postgresIngest = DAG(dag_id='postgresIngest'
                    , start_date=days_ago(1)
                    , default_args=default_args
                    , tags=['dphi']
                    , schedule_interval=None)

with postgresIngest as dag:

    checkFile = FileSensor(task_id = 'checkfile', filepath=f'{airflow_home}/data/*.csv', poke_interval=15, timeout=30)

    @task()
    def getFile():
        files = [file for file in os.listdir(f'{airflow_home}/data/') if Path(file).suffix == '.csv']
        # process(files[0])
        return files[0] if len(files) > 0 else None

    @task()
    def process(filename):
        print("processing file ")
        print(filename)
        IngestData.IngestDataToPostgres(f'{airflow_home}/data/{filename}', Path(filename).stem, 'dphi')
        print('moving file')
        shutil.move(f'{airflow_home}/data/{filename}', f'{airflow_home}/data/Processed/{filename}')

    process(getFile()).set_upstream(checkFile)

