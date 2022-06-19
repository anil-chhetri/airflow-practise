from fileinput import filename


from airflow import DAG
from airflow.decorators import task

from airflow.utils.dates import days_ago

import os
import pathlib as p

import IngestData as id

airflow_home = os.environ.get("AIRFLOW_HOME")

default_args = {
    "owner" : "anil",
    "retries": 0
}

root = DAG(dag_id="pipeline"
            , schedule_interval=None
            , start_date=days_ago(1)
            , catchup=False
            , tags=["dphi","ETL"]
        )


with root as dag:
    @task
    def get_files(location):
        files = [(str(x), p.PurePosixPath(x).stem) for x in p.Path(location).iterdir() if x.is_file() and x.suffix == '.csv']
        print(files)
        return files

    @task
    def add_to_postgres(file):
        filePath , fileName = file
        print(f'starting to add data to postgres: {filePath}')
        print(fileName)
        # id.IngestDataToPostgres(filePath, fileName, "dphi")     
        print("compoleted.")

    files = get_files(f'{airflow_home}/data/')
    add_to_postgres.expand(file = files)

