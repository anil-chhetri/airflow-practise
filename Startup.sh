# docker-compose up airflow-init && docker-compose up

AIRFLOW_HOME=`pwd`
export AIRFLOW_HOME 
#!/bin/bash
curpath=`pwd`
file="${curpath}/Startup.sh"

if [ -f "$file" ]; then
    export AIRFLOW_HOME=`pwd`
    echo $AIRFLOW_HOME
    echo "$AIRFLOW_HOME exist."
    echo "good to go"
else 
    echo "$file does not exist."

fi

