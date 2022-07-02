from sqlalchemy import create_engine
from time import time
from airflow import DAG

from airflow.decorators import task_group
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import os
import pandas as pd
import pyarrow.parquet as pq


airflow_home = os.environ.get('AIRFLOW_HOME')
base_url = "https://nyc-tlc.s3.amazonaws.com/trip+data/"
file_name = "fhv_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}"
url = base_url + file_name + '.parquet'


default_args = {
    "owner": "anil",
    "retires": 0,
    "depends_on_past": True
}


def convertToCSV(fileName):
    path = f"{airflow_home}/data/{fileName}"
    print(path)
    parquetfilepath = f"{path}.parquet"
    print(parquetfilepath)
    # df = pd.read_parquet(f"{path}.parquet")
    # df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    # df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
    arrow_dataset = pq.ParquetDataset(parquetfilepath)
    arrow_table = arrow_dataset.read()
    df = arrow_table.to_pandas(safe=False)

    print('converted.')
    df.to_csv(f"{path}.csv", index=False)
    print('conversion finsihed.')


def loadToPostgres(path, tableName, databaseName):
    postgres_conn = f"postgresql+psycopg2://postgres:postgres@localhost/{databaseName}"
    connection = create_engine(postgres_conn)
    print(path)

    df_iter = pd.read_csv(path,
                          chunksize=990000, iterator=True)
    df = next(df_iter)

    df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)

    print('create table')
    # create table.
    df.head(0).to_sql(tableName, con=connection, if_exists='replace')
    t_start = time()
    df.to_sql(tableName, con=connection, if_exists='append')
    duration = time() - t_start
    print(f'inserted chunk, took {(duration/60):3f} seconds')

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
            df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
            df.to_sql(name=tableName, con=connection, if_exists='append')
            duration = time() - t_start
            print(f'inserted next chucnk, took {(duration/60):3f} seconds')
        except StopIteration:
            break

    print('insertion completed.')


FHVIngest = DAG(dag_id="FHVIngest", schedule_interval="0 0 1 * *", catchup=True, start_date=datetime(
    2019, 1, 1), end_date=datetime(2019, 12, 5), concurrency=2, max_active_runs=2, tags=["dphi"])

with FHVIngest as dag:

    get_FHV_data = BashOperator(
        task_id="get_fhv_data", bash_command=f"""wget {url} -O {airflow_home}/data/{file_name}.parquet""")
    # , bash_command='echo "test"')

    convert_to_csv = PythonOperator(
        task_id="Convert_to_csv",
        provide_context=True,
        python_callable=convertToCSV,
        op_kwargs={"fileName": file_name}
    )

    load_postgres = PythonOperator(
        task_id="load_to_postgres",
        provide_context=True,
        python_callable=loadToPostgres,
        op_kwargs={"path": f"{airflow_home}/data/{file_name}.csv",
                   "tableName": file_name, "databaseName": "dphi"}
    )

    @task_group()
    def cleanup():
        csv_file_remove = BashOperator(
            task_id="removecsv", bash_command="rm ${AIRFLOW_HOME}/data/fhv_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}.csv")
        parquet_file_remove = BashOperator(
            task_id="removeparquet", bash_command="rm ${AIRFLOW_HOME}/data/fhv_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}.parquet")

    bash = cleanup()

get_FHV_data >> convert_to_csv >> load_postgres >> bash
