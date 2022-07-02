from sqlalchemy import create_engine

engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/dphi'.format(
        user='anilchhetri',
        password='Qwerty@CN#600',
        account_identifier='dl01586.central-india.azure',
    )
)
try:
    connection = engine.connect()
    results = connection.execute('select current_version()').fetchone()
    print(results[0])
finally:
    connection.close()
    engine.dispose()


# export AIRFLOW_CONN_SNOWFLAKE_new = 'snowflake://anilchhetri:Qwerty@CN#600@dl01586.central-india.azure'
