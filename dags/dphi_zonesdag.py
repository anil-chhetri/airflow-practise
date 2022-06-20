from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os
from datetime import datetime

from sqlalchemy import create_engine
import pandas as pd

airflow_home = os.environ.get('AIRFLOW_HOME')


def loadData(path, tablename, databasename):

    postgres_conn = f"postgresql+psycopg2://postgres:postgres@localhost/{databasename}"
    connection = create_engine(postgres_conn)
    print(path)

    df = pd.read_csv(path)
    df.to_sql(tablename, con=connection, if_exists='replace')
    print('load complete.')


ingest_zones = DAG(
    dag_id="ignest_zones",
    schedule_interval=None,
    start_date=datetime(2019, 1, 1),
    tags=["dphi"]
)

with ingest_zones as dag:

    get_data = BashOperator(
        task_id="get_data",
        bash_command=f"wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv -O {airflow_home}/data/zones.csv "
    )

    load_data = PythonOperator(
        task_id="load_data",
        provide_context=True,
        python_callable=loadData,
        op_kwargs={"path": f"{airflow_home}/data/zones.csv",
                   "tablename": "zones", "databasename": "dphi"}
    )

    remove_file = BashOperator(
        task_id="remove_file",
        bash_command="rm ${AIRFLOW_HOME}/data/zones.csv"
    )

    get_data >> load_data >> remove_file
