from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.decorators import task, task_group

from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator


defaultargs = {
    "owner": "anil",
    "retries": 0
}

dynamicTask = DAG(dag_id="dynamic_task"
                    , schedule_interval=None
                    , start_date=days_ago(1)
                    , catchup=False
                    , default_args= defaultargs
                    , tags=['dynamic mapping'])

with dynamicTask as dag:

    @task_group
    def simple_dynamic_Mapping():
        @task
        def add_one(x):
            return x + 1

        @task
        def sum_id(values):
            total = sum(values)
            print(f'total was {total}')

        added_values = add_one.expand(x=[1,2,3])
        sum_id(added_values)
    
    simple = simple_dynamic_Mapping()


    @task_group
    def dynamic_mapping_constant():
        @task
        def add(x, y):
            return x + y

        @task
        def get_result(values):
            total = sum(values)
            print(f"total value obtained {total}")

        addedValues = add.partial(y=10).expand(x =[1,2,3])
        result = get_result(addedValues)

    constant = dynamic_mapping_constant()



    @task_group
    def mapping_with_bash():
        
        (BashOperator
            .partial(task_id="bashOperator")
            .expand(bash_command = ["echo 'hello'", 'echo "hello2"' ]))

    bash = mapping_with_bash()

    

    simple >> constant >> bash