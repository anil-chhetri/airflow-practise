from airflow.hooks.base import BaseHook
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresDataBackup(BaseHook):

    def __init__(self, postgres_conn_id, table_name, backup_tablename, database, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.tablename = table_name
        self.backup_tablename = backup_tablename
        self.database = database

    def create_backupTable(self):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        hook.run(f"drop table if exists {self.backup_tablename}")
        sql = f"select * into {self.backup_tablename} from {self.tablename} where 1=2;"
        hook.run(sql)
        

    def backup(self):
        hook = PostgresHook(postgres_conn_id = self.postgres_conn_id, schema=self.database)
        self.log.info(f'Getting data from {self.tablename}.')
        data = hook.get_records(f"select * from {self.tablename};")
        self.log.info(f'inserting data to {self.backup_tablename}')

        hook.insert_rows(table=self.backup_tablename, rows=data)
        


class PostgresPlugins(AirflowPlugin):
    name='postgresPlugins',
    hooks = [PostgresDataBackup]

