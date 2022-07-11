"""
    trigger rules paramter defines if the upstream task was successfull or not. and 
    on the basis of upstream state the current task will choose to run or skip.

    we can have following values as the parameter.

    all_success: all parents have succeeded.
    all_failed: all the parents are in a failded or upstream failed state
    all_done: all parents are done with executions.

    one_failed: fires as soon as at least one parent as failed, it doesn't wait for all parents to be done.
    one_success: fires as soon as at least one parent succeeds, it doesn't wait for all parents to be done.

    none_failed: all parents have not failed. all parents either succeeded or skipped.
    none_skipped: no parents hvae skipped. 



"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.helpers import cross_downstream
from airflow.utils.trigger_rule import TriggerRule


from datetime import datetime

dag = DAG(
    dag_id="trigger_rule_test",
    schedule_interval='@daily',
    start_date=datetime(2022, 7, 10),
    catchup=False,
    tags=['mark']
)

with dag as d:

    extracct_a = BashOperator(
        task_id="extract_a",
        bash_command="echo '{{ ti.priority_weight }}'"
    )

    extract_b = BashOperator(
        task_id="extract_b",
        bash_command="echo '{{ ti.priority_weight }}'"
    )

    process_a = BashOperator(
        task_id="process_a",
        bash_command="echo '{{ ti.priority_weight }}' && sleep 10",
        pool='Experiment_pool',
        priority_weight=3
    )

    process_b = BashOperator(
        task_id="process_b",
        bash_command="echo '{{ ti.priority_weight }}' && sleep 10",
        pool='Experiment_pool',
        priority_weight=1
    )

    process_c = BashOperator(
        task_id="process_c",
        bash_command="echo '{{ ti.priority_weight }}' && sleep 10 && exit 1",
        pool='Experiment_pool',
        priority_weight=5
    )

    store = BashOperator(
        task_id='store',
        bash_command="echo 'completed.'"
    )

    clean_a = BashOperator(
        task_id="clean_a",
        bash_command='echo "test1" && sleep 5',
        trigger_rule=TriggerRule.ONE_FAILED
    )

    clean_b = BashOperator(
        task_id="clean_b",
        bash_command="echo 'test2' && sleep 5",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    clean_c = BashOperator(
        task_id="clean_c",
        bash_command="echo 'test3' && sleep 5 ",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    cross_downstream([extracct_a, extract_b], [
                     process_a, process_b, process_c])
    process_a >> clean_a
    process_b >> clean_b
    process_c >> clean_c
    [process_a, process_b, process_c] >> store
