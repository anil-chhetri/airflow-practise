import airflow
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pathlib
import json
import urllib.request as r


def _get_pictures():
    
    # ensure that the direcotry exists 
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    #download all the images in json.
    with open("/tmp/launches.json") as f:
        lauches = json.load(f)
        images_urls = [x['image'] for x in lauches['results']]
        print(images_urls)
        for image_url in images_urls:
            filename = image_url.split('/')[-1]
            filepath = pathlib.Path("/tmp/images").joinpath(filename)
            # print(filename)
            r.urlretrieve(image_url, filepath)






dag = DAG(
    dag_id="download_rocket_launches",
    start_date= airflow.utils.dates.days_ago(2),
    schedule_interval = None,
    tags=['book']
)


download_launches = BashOperator(
    task_id='Download_Launches',
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag
)


get_pictures = PythonOperator(
    task_id = 'get_pictures',
    python_callable= _get_pictures
)


notify = BashOperator(
    task_id='notify',
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."'
)

download_launches >> get_pictures >> notify
