import pandas as pd
from sqlalchemy import create_engine
from time import time


def IngestDataToPostgres(filename, tableName, databaseName):
    postgres_conn = f"postgresql+psycopg2://postgres:admin@localhost/{databaseName}"
    connection = create_engine(postgres_conn)
    print(filename)

    df_iter = pd.read_csv('yellow_taxi.csv',
                          chunksize=500000, iterator=True)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print('create table')
    # create table.
    df.head(0).to_sql(tableName, con=connection, if_exists='replace')
    t_start = time()
    df.to_sql(tableName, con=connection, if_exists='append')
    duration = time() - t_start
    print(f'inserted chunk, took {duration} seconds')

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=tableName, con=connection, if_exists='append')
            duration = time() - t_start
            print(f'inserted next chucnk, took {duration/60:3f} seconds')
        except StopIteration:
            break

    print('insertion completed.')
