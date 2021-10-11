#Build a DAG that will insert the students data like their id, and name into some MySQL table. And then copy that source table into some backup table.

from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from postgres_plugins import PostgresDataBackup

import airflow.utils.dates as d




with DAG(
    dag_id='assignment2',
    start_date= d.days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assignment 2', 'Postgres Operator', 'File Sensor']
) as d:
    
    filesensor = FileSensor(
        task_id = 'Check_file_exists',
        filepath='/home/anil/del/data/Student.csv',
        fs_conn_id='fs_default',
        poke_interval=10,
        timeout = 5*60
    )


    createTable = PostgresOperator(
        task_id = 'create_table_if_exists',
        sql='''
            DROP TABLE IF EXISTS student;
            CREATE TABLE IF NOT EXISTS Student(
                id integer not null primary key,
                firstname varchar(200) not null,
                lastname varchar(200) not null,
                email varchar(200) not null,
                gender varchar(200) not null
            );
        ''',
        postgres_conn_id='postgres_default',
        database='bookdata'
    )


    insertData = PostgresOperator(
        task_id = 'insert_data',
        postgres_conn_id='postgres_default',
        database='bookdata',
        sql='''
            COPY student from '/home/anil/del/data/Student.csv' DELimiter ',' CSV HEADER;
        '''
    )

    def _backup():
        database = 'bookdata'
        postgres_conn_id='postgres_default'
        table_name = 'student'
        backup_tablename='student_bak'
        tablebackup = PostgresDataBackup(postgres_conn_id, table_name, backup_tablename, database)
        tablebackup.create_backupTable()
        tablebackup.backup()

    backup = PythonOperator(
        task_id = 'backup', 
        python_callable=_backup
    )

    filesensor >> createTable >> insertData >> backup