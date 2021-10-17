from airflow.models import DAG
import airflow.utils.dates as d
from airflow.utils.task_group import TaskGroup

from airflow.operators.sql import SQLCheckOperator, SQLValueCheckOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id="SQL_operations",
    schedule_interval=None,
    start_date= d.days_ago(1),
    catchup=False,
    template_searchpath=["/home/'airflow practise'/airflow-practise/include/sql", '/home/anil/test'],
    tags=['SQL', 'CheckOperator']
) as d:


    start = DummyOperator(task_id='start')
            
    with TaskGroup(group_id='sqlcheck') as task_sqlcheck:

        '''
            The SQLCheckOperator returns a single row from a provided SQL query and checks to see if 
            any of the returned values in that row are False. If any values are False, the task fails. 
        '''
        check_operator_success = SQLCheckOperator(
                task_id='checkOperator_sucess',
                sql='check1.sql',
                conn_id='postgres_default',
                database ='bookdata'
        )


        check_operator_fail = SQLCheckOperator(
            task_id = 'checkOperator_fail',
            sql = 'check2.sql',
            conn_id='postgres_default',
            database='bookdata'

        )


    intermediate = DummyOperator(task_id = 'intermediate1', trigger_rule = 'all_done')

    with TaskGroup(group_id='sqlValueCheck') as t2:

        '''
            It checks the results of a query against a specific pass value, and ensures the checked 
            value is within a percentage threshold of the pass value. 
            The pass value can be any type, 
            but the threshold can only be used with numeric types.
        '''

        valueCheck_success = SQLValueCheckOperator(
            task_id= 'valuecheck_sucess1',
            sql = 'select count(1) from student;',
            pass_value=1000,
            tolerance=1,
            database='bookdata',
            conn_id='postgres_default'
        )


        valueCheck_success = SQLValueCheckOperator(
            task_id= 'valuecheck_sucess2',
            sql = 'select count(1) from student;',
            pass_value=999,
            tolerance=0.1,
            database='bookdata',
            conn_id='postgres_default'
        )



    intermediate2 = DummyOperator(task_id = 'intermediate2', trigger_rule = 'all_done')


start >> task_sqlcheck >> intermediate >> t2 >> intermediate2