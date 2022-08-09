'''
using sql file without using template_searchpath
'''

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime

dag = DAG(dag_id="my_postgres_dag", schedule_interval=None, start_date=datetime(
    2022, 8, 9), catchup=False, tags=["postgres", "SQL"])

with dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_default",
        # sql="Create table if not exists my_table(table_value varchar(500))"
        sql="sql/postgres/create_my_table.sql"
    )

    store = PostgresOperator(
        task_id="store",
        postgres_conn_id="postgres_default",
        sql="sql/postgres/Insert_into_my_table.sql"
    )

    create_table >> store
