from airflow.decorators import tasks
from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import os
import pandas as pd


base_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/" 
yellow_file_name = "yellow_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}"
yellow_taxi_url = base_url + yellow_file_name + ".parquet"
# green_taxi_url = base_url + 
green_file_name = "green_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}"
green_taxi_url = base_url + green_file_name + ".parquet"



airflow_home =  os.environ.get('AIRFLOW_HOME')




def convertToCSV(filename, path):
    fullpath = f'{path}{filename}.parquet'
    print(fullpath)
    df = pd.read_parquet(f'{path}{filename}.parquet')
    df.to_csv(f'{path}/{filename}.csv', index=False)
    print("convert finished.")
    # return 1
    


default_args = {
    "owner": "anil",
    "retries" : 0, 
    "depends_on_past":  False
}

pipelineDag = DAG(dag_id= "pipelineDag"
                    , start_date= datetime(2019, 1,1)
                    , end_date=datetime(2019,3,30)
                    , catchup=True
                    , schedule_interval= "0 0 1 * *"
                    , default_args=default_args
                    , tags= ["dphi", 'bootcamp']
                    )


with pipelineDag as dag:

    # get_data_yellow_taxi = BashOperator(
    #               task_id = "get_data"
    #             , bash_command= f"wget {yellow_taxi_url} -O '{airflow_home}/data/{yellow_file_name}.parquet'"
    #             # , bash_command="echo 'test'"
    #             )

    # get_data_green_taxi = BashOperator(
    #               task_id = "get_data"
    #             , bash_command= f"wget {yellow_taxi_url} -O '{airflow_home}/data/{yellow_file_name}.parquet'"
    #             # , bash_command="echo 'test'"
    #             )


    @tasks
    def get_list():
        file = []
        file.append((yellow_taxi_url, yellow_taxi_name ))
        print(file)

    convert_to_csv = PythonOperator(
        task_id="convert_to_csv",
        provide_context = True,
        python_callable=convertToCSV,
        op_kwargs= { "filename": yellow_file_name, "path": f"{airflow_home}/data/"}
    )

    get_list() >> convert_to_csv
    # get_data_yellow_taxi >> convert_to_csv




    #https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.parquet
    #wget http://www.example.com/filename.txt -o /path/filename.txt


