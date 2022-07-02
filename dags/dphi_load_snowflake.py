from asyncio import Task, create_task
from airflow import DAG

from airflow.operators.bash import BashOperator
# from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalFilesystemToADLSOperator
# from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from airflow.operators.python import PythonOperator

from azure.storage.blob import BlobServiceClient

from datetime import datetime
import os
from pathlib import Path


default_args = {
    "owner": "anil",
    "depends_on_past": False,
    "retries": 0
}

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')
path_to_sql = f"{AIRFLOW_HOME}/include/sql/snowflake/initialprep.sql"
connect_str = "**secret"


def upload_file(local_csv_path):
    filename = Path(local_csv_path).name
    print('executing python code...')
    # connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    # //code from container level rather and blob level.

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # upload_file_path = f"{AIRFLOW_HOME}/data/yellow_tripdata_2019-01.parquet"

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(
        container='datastores/raw', blob=filename)

    with open(local_csv_path, "rb") as data:
        blob_client.upload_blob(data)

    print('complete.')


def delete_file(containerName, filepath):
    filename = Path(filepath).stem
    filename = Path('raw').joinpath(filename)
    print(filename)

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    print('getting client to delete')
    blob_client = blob_service_client.get_container_client(containerName)
    print(blob_client)
    print(filename)
    print('getting client')
    blob_list = blob_client.list_blobs(name_starts_with=filename)
    for blob in blob_list:
        print(blob.name)
        blob_client.delete_blob(blob)

    print('delete completed.')


def load_to_snowflake(dag, url_template, local_csv_path):
    filename = Path(local_csv_path).name

    with dag:

        download_dataset = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"wget {url_template} -O {local_csv_path}"
            # f"wget {yellow_taxi_url} -O '{airflow_home}/data/{yellow_file_name}.parquet'"
        )

        upload_to_azure_blob = PythonOperator(
            task_id="upload_azure",
            python_callable=upload_file,
            op_kwargs={"local_csv_path": local_csv_path}
        )

        create_stage = SnowflakeOperator(
            task_id="create_stage",
            sql='initialprep.sql',
            # parameters={"filename": "yellow_tripdata_2019-01.parquet"},
            snowflake_conn_id='snowflake_default',
            warehouse='transforming',
            database='raw',
            schema='dphi',
            role='ACCOUNTADMIN'
        )

        create_table = SnowflakeOperator(
            task_id="create_table",
            sql=f'create or replace table "{Path(local_csv_path).stem}" ( data variant);',
            warehouse="transforming",
            database="raw",
            schema="dphi",
            role="AccountAdmin"
        )

        insert_data = SnowflakeOperator(
            task_id="insert_data",
            sql=f'''
            truncate table "{Path(local_csv_path).stem}";
            insert into "{Path(local_csv_path).stem}" select $1 from @azure_data_ingestion_stage(FILE_FORMAT => 'my_parquet_format', PATTERN => '.{Path(local_csv_path).name}')''',
            warehouse='transforming',
            database="raw",
            schema="dphi",
            role="accountAdmin"
        )

        delete_blob = PythonOperator(
            task_id="delete_blob",
            python_callable=delete_file,
            op_kwargs={"containerName": "datastores",
                       "filepath": local_csv_path}
        )

        # insert into select $1 from @ azure_data_ingestion_stage(FILE_FORMAT= > 'my_parquet_format', PATTERN = > '.yellow_tripdata_2019-01.parquet')

        download_dataset >> upload_to_azure_blob >> create_stage >> create_table >> insert_data >> delete_blob


URL_PREFIX = 'https://nyc-tlc.s3.amazonaws.com/trip+data'
# https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-01.parquet

YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + \
    '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + \
    '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'

local_path = AIRFLOW_HOME + \
    "/data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

yellow_taxi_data_dag = DAG(
    dag_id='yellow_taxi_data_v2',
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 2, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
    template_searchpath=f'{AIRFLOW_HOME}/include/sql/snowflake',
    tags=["dphi", "date_eng"]
)


load_to_snowflake(dag=yellow_taxi_data_dag,
                  url_template=YELLOW_TAXI_URL_TEMPLATE, local_csv_path=local_path)


# export AIRFLOW_CONN_AZURE_DATA_LAKE_DEFAULT = 'azure-data-lake://4227ae71-8f7c-4ff1-af1a-214654bdddb4:4rV8Q~nCi_NEnpPMhWIEfYthACbYJjHZ5xFM6clj@?tenant=7680316a-5e9e-4cc5-a52e-9ee89ee8c404&account_name=datastores'

# export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=dphidatastorage;AccountKey=KF5twYiLTqk/B7iAEZm1Uv7Fbe9p6bi6h2D0Mqfv32EvlbLpiAXKmhJjl4rR0OyWQjBwRm+mky0D+AStCWeIbA==;EndpointSuffix=core.windows.net"
