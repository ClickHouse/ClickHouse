#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import enum
import logging
import random
import sqlite3
import string
from contextlib import contextmanager

import pyodbc  # pylint:disable=import-error; for style check

from exceptions import ProgramError

logger = logging.getLogger("connection")
logger.setLevel(logging.DEBUG)


class OdbcConnectingArgs:
    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __str__(self):
        conn_str = ";".join([f"{x}={y}" for x, y in self._kwargs.items() if y])
        return conn_str

    def update_database(self, database):
        self._kwargs["Database"] = database

    @staticmethod
    def create_from_kw(
        dsn="", server="localhost", user="default", database="default", **kwargs
    ):
        conn_args = {
            "DSN": dsn,
            "Server": server,
            "User": user,
            "Database": database,
        }
        conn_args.update(kwargs)
        return OdbcConnectingArgs(**conn_args)

    @staticmethod
    def create_from_connection_string(conn_str):
        args = OdbcConnectingArgs()
        for kv in conn_str.split(";"):
            if kv:
                k, v = kv.split("=", 1)
                # pylint:disable-next=protected-access
                args._kwargs[k] = v
        return args


def _random_str(length=8):
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.SystemRandom().choice(alphabet) for _ in range(length))


def default_clickhouse_odbc_conn_str():
    return str(
        OdbcConnectingArgs.create_from_kw(
            dsn="ClickHouse DSN (ANSI)",
            Timeout="300",
            Url="http://localhost:8123/query?default_format=ODBCDriver2&"
            "default_table_engine=MergeTree&union_default_mode=DISTINCT&"
            "group_by_use_nulls=1&join_use_nulls=1&allow_create_index_without_type=1&"
            "create_index_ignore_unique=1",
        )
    )


class Engines(enum.Enum):
    SQLITE = enum.auto()
    ODBC = enum.auto()

    @staticmethod
    def list():
        return list(map(lambda c: c.name.lower(), Engines))


class KnownDBMS(str, enum.Enum):
    sqlite = "sqlite"
    clickhouse = "ClickHouse"


class ConnectionWrap:
    def __init__(self, connection=None, factory=None, factory_kwargs=None):
        self._factory = factory
        self._factory_kwargs = factory_kwargs
        self._connection = connection

        self.DBMS_NAME = None
        self.DATABASE_NAME = None
        self.USER_NAME = None

    @staticmethod
    def create(connection):
        return ConnectionWrap(connection=connection)

    @staticmethod
    def create_form_factory(factory, factory_kwargs):
        return ConnectionWrap(
            factory=factory, factory_kwargs=factory_kwargs
        ).reconnect()

    def can_reconnect(self):
        return self._factory is not None

    def reconnect(self):
        if self._connection is not None:
            self._connection.close()
        self._connection = self._factory(self._factory_kwargs)
        return self

    def assert_can_reconnect(self):
        assert self.can_reconnect(), f"no reconnect for: {self.DBMS_NAME}"

    def __getattr__(self, item):
        return getattr(self._connection, item)

    def __enter__(self):
        return self

    def drop_all_tables(self):
        if self.DBMS_NAME == KnownDBMS.clickhouse.value:
            list_query = (
                f"SELECT name FROM system.tables WHERE database='{self.DATABASE_NAME}'"
            )
        elif self.DBMS_NAME == KnownDBMS.sqlite.value:
            list_query = "SELECT name FROM sqlite_master WHERE type='table'"
        else:
            logger.warning(
                "unable to drop all tables for unknown database: %s", self.DBMS_NAME
            )
            return

        list_result = execute_request(list_query, self)
        logger.info("tables will be dropped: %s", list_result.get_result())
        for table_name in list_result.get_result():
            table_name = table_name[0]
            execute_request(f"DROP TABLE {table_name}", self).assert_no_exception()
            logger.debug("success drop table: %s", table_name)

    def _use_database(self, database="default"):
        if self.DBMS_NAME == KnownDBMS.clickhouse.value:
            logger.info("use test database: %s", database)
            self._factory_kwargs.update_database(database)
            self.reconnect()
            self.DATABASE_NAME = database

    def use_random_database(self):
        if self.DBMS_NAME == KnownDBMS.clickhouse.value:
            database = f"test_{_random_str()}"
            execute_request(f"CREATE DATABASE {database}", self).assert_no_exception()
            self._use_database(database)
            logger.info(
                "currentDatabase : %s",
                execute_request("SELECT currentDatabase()", self).get_result(),
            )

    @contextmanager
    def with_one_test_scope(self):
        try:
            yield self
        finally:
            self.drop_all_tables()

    @contextmanager
    def with_test_database_scope(self):
        self.use_random_database()
        try:
            yield self
        finally:
            self._use_database()

    def __exit__(self, *args):
        if hasattr(self._connection, "close"):
            self._connection.close()


def setup_connection(engine, conn_str=None, make_debug_request=True):
    connection = None

    if isinstance(engine, str):
        engine = Engines[engine.upper()]

    if engine == Engines.ODBC:
        if conn_str is None:
            raise ProgramError("conn_str has to be set up for ODBC connection")

        logger.debug("Drivers: %s", pyodbc.drivers())
        logger.debug("DataSources: %s", pyodbc.dataSources())
        logger.debug("Connection string: %s", conn_str)

        conn_args = OdbcConnectingArgs.create_from_connection_string(conn_str)

        connection = ConnectionWrap.create_form_factory(
            factory=lambda args: pyodbc.connect(str(args)),
            factory_kwargs=conn_args,
        )
        connection.add_output_converter(pyodbc.SQL_UNKNOWN_TYPE, lambda x: None)

        connection.DBMS_NAME = connection.getinfo(pyodbc.SQL_DBMS_NAME)
        connection.DATABASE_NAME = connection.getinfo(pyodbc.SQL_DATABASE_NAME)
        connection.USER_NAME = connection.getinfo(pyodbc.SQL_USER_NAME)

    elif engine == Engines.SQLITE:
        conn_str = conn_str if conn_str is not None else ":memory:"
        connection = ConnectionWrap.create(sqlite3.connect(conn_str))

        connection.DBMS_NAME = "sqlite"
        connection.DATABASE_NAME = "main"
        connection.USER_NAME = "default"

    logger.info(
        "Connection info: DBMS name %s, database %s, user %s",
        connection.DBMS_NAME,
        connection.DATABASE_NAME,
        connection.USER_NAME,
    )

    if make_debug_request:
        request = "SELECT 1"
        logger.debug("Make debug request to the connection: %s", request)
        result = execute_request(request, connection)
        logger.debug("Debug request returned: %s", result.get_result())

    logger.debug("Connection is ok")
    return connection


class ExecResult:
    def __init__(self):
        self._exception = None
        self._result = None
        self._description = None

    def as_exception(self, exc):
        self._exception = exc
        return self

    def get_result(self):
        self.assert_no_exception()
        return self._result

    def get_description(self):
        self.assert_no_exception()
        return self._description

    def as_ok(self, rows=None, description=None):
        if rows is None:
            self._result = []
            return self
        self._result = rows
        self._description = description
        return self

    def get_exception(self):
        return self._exception

    def has_exception(self):
        return self._exception is not None

    def assert_no_exception(self):
        if self.has_exception():
            raise ProgramError(
                "request doesn't have a result set, it has the exception",
                parent=self._exception,
            )


def execute_request(request, connection):
    cursor = connection.cursor()
    try:
        cursor.execute(request)
        if cursor.description:
            logging.debug("request has a description %s", cursor.description)
            rows = cursor.fetchall()
            connection.commit()
            return ExecResult().as_ok(rows=rows, description=cursor.description)
        logging.debug("request doesn't have a description")
        connection.commit()
        return ExecResult().as_ok()
    except (pyodbc.Error, sqlite3.DatabaseError) as err:
        return ExecResult().as_exception(err)
    finally:
        cursor.close()
