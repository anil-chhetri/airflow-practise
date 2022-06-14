# docker-compose up airflow-init && docker-compose up

AIRFLOW_HOME=`pwd`
export AIRFLOW_HOME 
#!/bin/bash
curpath=`pwd`
file="${curpath}/Startup.sh"
#echo "${file}"



if [ -f "$file" ]; then
    export AIRFLOW_HOME=`pwd`
    echo $AIRFLOW_HOME
    echo "$AIRFLOW_HOME exist."

