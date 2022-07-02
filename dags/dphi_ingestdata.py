from airflow.decorators import task, task_group
from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import os
import pandas as pd

from IngestData import IngestDataToPostgres

base_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
yellow_file_name = "yellow_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}"
yellow_taxi_url = base_url + yellow_file_name + ".parquet"
# green_taxi_url = base_url +
green_file_name = "green_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}"
green_taxi_url = base_url + green_file_name + ".parquet"


airflow_home = os.environ.get('AIRFLOW_HOME')
pathwithfileName = f"{airflow_home}/data/{yellow_file_name}"


def test(filename, tableName, databaseName):
    pass


def convertToCSV(filename, path):
    fullpath = f'{path}{filename}.parquet'
    print(fullpath)
    df = pd.read_parquet(f'{path}{filename}.parquet')
    df.to_csv(f'{path}/{filename}.csv', index=False)
    print("convert finished.")
    # return 1


default_args = {
    "owner": "anil",
    "retries": 0,
    "depends_on_past":  True
}

pipelineDag = DAG(dag_id="pipelineDag", start_date=datetime(2019, 1, 1), end_date=datetime(2020, 12, 5), catchup=True, concurrency=2, max_active_runs=2, schedule_interval="0 0 1 * *", default_args=default_args, tags=["dphi", 'bootcamp']
                  )


with pipelineDag as dag:

    get_data_yellow_taxi = BashOperator(
        task_id="get_data", bash_command=f"wget {yellow_taxi_url} -O '{airflow_home}/data/{yellow_file_name}.parquet'"
        # , bash_command="echo 'test'"
    )

    # get_data_green_taxi = BashOperator(
    #               task_id = "get_data_green"
    #             , bash_command= f"wget {green_taxi_url} -O '{airflow_home}/data/{green_file_name}.parquet'"
    #             # , bash_command="echo 'test'"
    #             )

    convert_to_csv_yellow = PythonOperator(
        task_id="convert_to_csv",
        provide_context=True,
        python_callable=convertToCSV,
        op_kwargs={"filename": yellow_file_name,
                   "path": f"{airflow_home}/data/"}
    )

    load_to_postgres_yellow = PythonOperator(
        task_id="load_yellow_taxi",
        provide_context=True,
        python_callable=IngestDataToPostgres,
        op_kwargs={"filename": f"{airflow_home}/data/{yellow_file_name}.csv",
                   "tableName": yellow_file_name, "databaseName": "dphi"}
        # filename, tableName, databaseName
    )

    @task_group()
    def cleanup():
        # (BashOperator
        #     .partial(task_id="remove_files")
        #     .expand(bash_command=[
        #         "rm yellow_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}.csv",
        #         "rm yellow_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}.parquet"

        #     ]))

        csv_file_remove = BashOperator(
            task_id="removecsv", bash_command="rm ${AIRFLOW_HOME}/data/yellow_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}.csv")
        parquet_file_remove = BashOperator(
            task_id="removeparquet", bash_command="rm ${AIRFLOW_HOME}/data/yellow_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}.parquet")

    bash = cleanup()

    # convert_to_csv_green = PythonOperator(
    #     task_id="convert_to_csv_green",
    #     provide_context = True,
    #     python_callable=convertToCSV,
    #     op_kwargs= { "filename": green_file_name, "path": f"{airflow_home}/data/"}
    # )

    # load_to_postgres_green = PythonOperator(
    #     task_id = "load_green_taxi",
    #     provide_context = True,
    #     python_callable=IngestDataToPostgres,
    #     op_kwargs={"filename": f"{airflow_home}/data/{green_file_name}.csv", "tableName": green_file_name, "databaseName": "dphi"}
    #     # filename, tableName, databaseName
    # )

    # get_data_yellow_taxi >> convert_to_csv_yellow >> get_data_green_taxi >> convert_to_csv_green
    get_data_yellow_taxi >> convert_to_csv_yellow >> load_to_postgres_yellow >> bash
    # get_data_green_taxi >> convert_to_csv_green >> load_to_postgres_green
    # get_data_yellow_taxi >> convert_to_csv

    # https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.parquet
    # wget http://www.example.com/filename.txt -o /path/filename.txt
