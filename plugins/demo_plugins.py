from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.filesystem import FSHook

from airflow.utils.decorators import apply_defaults

import logging as log
import os


class DataTransferOperator(BaseOperator):

    def __init__(self, source_file_path, destination_file_path, delete_words, *args, **kwargs):
        
        self.source_file_path = source_file_path
        self.destination_file_path = destination_file_path
        self.delete_words = delete_words

        super().__init__(*args, **kwargs)


    def execute(self, context):

        source_file = self.source_file_path
        destination_file = self.destination_file_path
        delete_list = self.delete_words


        log.info('## custom operator execution starts. ## ')
        log.info(f'Source file {source_file}')
        log.info(f'destination file {destination_file}')
        log.info(f'delete list {delete_list}')

        fin = open(source_file, 'r')
        fout = open(destination_file, 'a')


        for line in fin:
            log.info(f'reading line: {line}')
            for word in delete_list:
                log.info(f'delete word {word}')
                line = line.lower().replace(word, '')

            fout.write(line)

        fin.close()
        fout.close()


class FileSensorCount(BaseSensorOperator):

    def __init__(self, file_conn, file_path,  *args, **kwargs):
        self.file_conn = file_conn
        self.file_path = file_path
        super().__init__(*args, **kwargs)


    def poke(self, context) -> bool:
        print('inside the poke method.')
        print('context value: ', context)

        hook = FSHook(self.file_conn)
        basepath = hook.get_path()
        self.log.info(f'path from hook, {basepath}')
        full_path = os.path.join(basepath, self.file_path)

        self.log.info(f'poking location {full_path}')

        try: 
            for root, dirs, files in os.walk(full_path):
                if len(files) >= 5: 
                    return True 
        except OSError:
            return False

        return False

class DemoPlugins(AirflowPlugin):
    name = 'demo_plugins'
    operators = [DataTransferOperator]
    sensors = [FileSensorCount]