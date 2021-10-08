import airflow.utils.dates as date

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from urllib import request


dag = DAG(
    dag_id='Python_templating',
    start_date= date.days_ago(2),
    schedule_interval=None,
    catchup=False,
    tags=['Book']
)

#https://dumps.wikimedia.org/other/pageviews/2021/2021-10/pageviews-20211008-020000.gz


'''
Python will then check if any of the given arguments is expected in the function signa-
ture of python callable, if there exists the keyword than it will pass the value to that key word
if not it will pass it through Kwargs dict.
'''

def _get_data(output_path ,**kwargs):
    print('from python callable')
    print(kwargs.get('execution_date'))
    year, month, day, hour, *_ = kwargs.get('execution_date').timetuple()
    print(year, month, day, hour)
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month}/pageviews-"
        f"{year}{month:0>2}{day:0>2}-{hour-1:0>2}0000.gz"

    )
    print(url)
    # output_path = '/tmp/wikipages.gz'

    '''
        getting ouput path from variable that is passed from python callable.

        The value for output_path can be provided in two ways. 
            The first is via an argument:  op_args
            The second approach is to use the op_kwargs argument
    '''
    request.urlretrieve(url, output_path)



'''
    On execution of the operator, each value in the list provided to op_args is passed along
    to the callable function (i.e., the same effect as calling the function as such directly:
    _get_data("/tmp/wikipageviews.gz") )

    Since output_path is the first argument in the _get_data function,
    the value of it will be set to /tmp/wikipageviews.gz when run (we call these non-keyword
    arguments)
'''

get_data = PythonOperator(
    task_id = 'get_data',
    python_callable=_get_data,
    # op_args=["/tmp/wikipages.gz"],
    op_kwargs={'output_path' : '/tmp/wikipages.gz'},
    dag=dag
)


'''
    Note that these values can contain strings and thus can be templated. That means we
    could avoid extracting the datetime components inside the callable function itself and
    instead pass templated strings to our callable function.
    or we could this also.

    to check the render templates 
        > airflow tasks render [dag_id] [task_id] [desired execution date]
'''

# def _get_data_02(output_path, year, month, day, hour):
#     print(output_path, year, month, day)
#     url = (
#         "https://dumps.wikimedia.org/other/pageviews/"
#         f"{year}/{year}-{month}/pageviews-"
#         f"{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"

#     )
#     print(url)
#     request.urlretrieve(url, output_path)

# get_data_02 = PythonOperator(
#     task_id = 'get_data_with_callable_template',
#     python_callable=_get_data_02,
#     op_kwargs={
#         "output_path" : "/tmp/wikipages.gz",
#         "year" : "{{ execution_date.year }}",
#         "month": "{{ execution_date.month }}",
#         "day" : "{{ execution_date.day }}",
#         "hour" : "{{ execution_date.hour -1 }}"
#     },
#     dag=dag
# )



extract_gz = BashOperator(
    task_id='extract_gz',
    bash_command="gunzip --force /tmp/wikipages.gz",
    dag=dag
)


def _fetchView(pagenames):
    result = dict.fromkeys(pagenames,0)
    with open(f"/tmp/wikipages", 'r') as f:  
        for line in f:
            domainCode, page_title, views_coutns, _ = line.split(" ")
            if domainCode == 'en' and page_title in pagenames:
                pagenames[page_title] = views_coutns

    print(pagenames)



fetch_pageViews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable= _fetchView ,
    op_kwargs={
        "pagename" : {
            "Google",
            "Amazon",
            "Apple",
            "Microsoft",
            "Facebook"
        }
    },
    dag=dag
)

get_data >> extract_gz >> fetch_pageViews