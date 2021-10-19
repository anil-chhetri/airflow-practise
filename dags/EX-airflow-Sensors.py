from airflow.models import DAG
from airflow import configuration
import airflow.utils.dates as d

from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor

from airflow.operators.python import PythonOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pathlib as p



airflow_root_path = p.Path(configuration.get_airflow_home())


default_args = {
    'owner' : 'anil'
}

with DAG(
    dag_id='FileSensors',
    start_date=d.days_ago(1),
    schedule_interval=None,
    default_args=default_args,
    tags=['Book', 'TriggerDagRunOperator']
) as d:
    

    '''  
        Checking for 1 files if its exists or not.
    '''
    file_sensor = FileSensor(
        task_id = 'FileSensor_single_file',
        filepath=airflow_root_path / 'include' / 'files' / 'test',
        poke_interval=10,
        timeout=500,
        mode='reschedule'
    )


    '''
        even though this sensor sense multiple files it doesn't wait for all the 
        files to be dump at the location so it is quite ineffective if all the 
        files are not dump at the job start time.
    '''
    file_sensor_multiple = FileSensor(
        task_id = 'Multiple_files',
        filepath=airflow_root_path / 'include' / 'files' / '1' / '*.csv',
        poke_interval = 15,
        timeout = 500,
        mode='reschedule'
    )


    def _check_files(path):
        csv_files = path.glob('*.csv')
        success_file = path / 'success'
        print([x for x in csv_files])
        print(success_file)
        return csv_files and success_file.exists()

    file_sensor_multiple_condition = PythonSensor(
        task_id = 'multiple_condition',
        python_callable=_check_files,
        op_kwargs={"path" : airflow_root_path.joinpath('include', 'files', '1') },
        poke_interval=10,
        timeout = 150
    )

    clean_dag = TriggerDagRunOperator(
        task_id = 'calling_clean_up_dag',
        trigger_dag_id='cleanup'
    )


    [file_sensor, file_sensor_multiple, file_sensor_multiple_condition] >> clean_dag



with DAG(
    dag_id='cleanup', 
    default_args=default_args,
    schedule_interval=None

) as d1: 

    def _deleteFiles():
        pass

    deleteFiles = PythonOperator(
        task_id = 'delete files',
        python_callable=_deleteFiles
    )
