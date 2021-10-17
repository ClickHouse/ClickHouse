#!/bin/python3
from clickhouse_driver import Client
import sys


class DBOrdinaryToAtomicConverter:
    def __init__(self):
        self.__atomic_prefix = '__temp_ordinary_to_atomic__'
        self.__client = Client('localhost')

    def __db_exist(self, db_name):
        dbs = self.__client.execute(f'SELECT * FROM system.databases WHERE name=\'{db_name}\'')
        return len(dbs) == 1

    def __db_engine(self, db_name):
        assert self.__db_exist(db_name), 'something went wrong'

        dbs = self.__client.execute(f'SELECT engine FROM system.databases WHERE name=\'{db_name}\'')
        return dbs[0][0]

    def __get_tables(self, db_name):
        tables = self.__client.execute(f'SELECT name FROM system.tables WHERE database == \'{db_name}\'')
        return tables

    def __continue_change(self, atomic_name):
        self.__rename_atomic_db(atomic_name)

    def __abort_change(self, atomic_name):
        self.__drop_database(atomic_name)

    def __create_atomic_db(self, ordinary_name):
        atomic_name = f'{self.__atomic_prefix}{ordinary_name}'

        if self.__db_exist(atomic_name):
            if self.__db_exist(self.__remove_atomic_prefix(atomic_name)):
                self.__abort_change(atomic_name)
                print('The last launch failed but ordinary table was restored')
            else:
                self.__continue_change(atomic_name)
                print('Changing continued after fail in previous launch, atomic table was restored')
            return

        self.__client.execute(f'CREATE DATABASE {atomic_name} ENGINE=Atomic')
        return atomic_name

    def __rename_table(self, old_name, new_name):
        self.__client.execute(f'RENAME TABLE {old_name} TO {new_name}')

    def __drop_database(self, db_name):
        self.__client.execute(f'DROP DATABASE {db_name}')

    def __remove_atomic_prefix(self, name):
        assert atomic_name.startswith(self.__atomic_prefix)

        prefix_len = len(self.__atomic_prefix)
        return name[prefix_len:]

    def __rename_atomic_db(self, atomic_name):
        ordinary_name = self.__remove_atomic_prefix(ordinary_name)
        self.__client.execute(f'RENAME DATABASE {atomic_name} TO {ordinary_name}')

    def convert(self, ordinary_name):
        assert self.__db_exist(ordinary_name), 'ordinary database does not exist'
        assert self.__db_engine(ordinary_name) == 'Ordinary', 'something went wrong'

        tables = self.__get_tables(ordinary_name)
        atomic_name = self.__create_atomic_db(ordinary_name)

        if atomic_name is None:
            return

        for table in tables:
            ordinary_table = f'{ordinary_name}.{table}'
            atomic_table = f'{atomic_name}.{table}'
            self.__rename_table(ordinary_table, atomic_table)

        self.__drop_database(ordinary_name)
        self.__rename_atomic_db(atomic_name)

        assert self.__db_engine(ordinary_name) == 'Atomic', 'something went wrong'


def main():
    converter = DBOrdinaryToAtomicConverter()

    dbs = sys.argv[1:]
    for db in dbs:
        converter.convert(db)


if __name__ == '__main__':
    main()
