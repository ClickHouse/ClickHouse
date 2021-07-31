import uuid

import testflows.settings as settings

from contextlib import contextmanager

from testflows.core.name import basename, parentname
from testflows._core.testtype import TestSubType
from testflows.core import *

from helpers.common import Pool, join, run_scenario, instrument_clickhouse_server_log
from rbac.helper.tables import table_types

def permutations(table_count=1):
    return [*range((1 << table_count)-1)]

def getuid():
    if current().subtype == TestSubType.Example:
        testname = f"{basename(parentname(current().name)).replace(' ', '_').replace(',','')}"
    else:
        testname = f"{basename(current().name).replace(' ', '_').replace(',','')}"
    return testname + "_" + str(uuid.uuid1()).replace('-', '_')

@contextmanager
def table(node, name, table_type_name="MergeTree"):
    table_type = table_types[table_type_name]
    try:
        names = name.split(",")
        for name in names:
            with Given(f"I have {name} with engine {table_type_name}"):
                node.query(f"DROP TABLE IF EXISTS {name}")
                node.query(table_type.create_statement.format(name=name))
        yield
    finally:
        for name in names:
            with Finally(f"I drop the table {name}"):
                if table_type.cluster:
                    node.query(f"DROP TABLE IF EXISTS {name} ON CLUSTER {table_type.cluster}")
                else:
                    node.query(f"DROP TABLE IF EXISTS {name}")

@contextmanager
def user(node, name):
    try:
        names = name.split(",")
        for name in names:
            with Given("I have a user"):
                node.query(f"CREATE USER OR REPLACE {name}")
        yield
    finally:
        for name in names:
            with Finally("I drop the user"):
                node.query(f"DROP USER IF EXISTS {name}")

@contextmanager
def role(node, role):
    try:
        roles = role.split(",")
        for role in roles:
            with Given("I have a role"):
                node.query(f"CREATE ROLE OR REPLACE {role}")
        yield
    finally:
        for role in roles:
            with Finally("I drop the role"):
                node.query(f"DROP ROLE IF EXISTS {role}")

@TestStep(Given)
def row_policy(self, name, table, node=None):
    """Create a row policy with a given name on a given table.
    """
    if node is None:
        node = self.context.node

    try:
        with Given(f"I create row policy {name}"):
            node.query(f"CREATE ROW POLICY {name} ON {table}")
        yield

    finally:
        with Finally(f"I delete row policy {name}"):
            node.query(f"DROP ROW POLICY IF EXISTS {name} ON {table}")

tables = {
    "table0" : 1 << 0,
    "table1" : 1 << 1,
    "table2" : 1 << 2,
    "table3" : 1 << 3,
    "table4" : 1 << 4,
    "table5" : 1 << 5,
    "table6" : 1 << 6,
    "table7" : 1 << 7,
}

@contextmanager
def grant_select_on_table(node, grants, target_name, *table_names):
    try:
        tables_granted = []
        for table_number in range(len(table_names)):

            if(grants & tables[f"table{table_number}"]):

                with When(f"I grant select privilege on {table_names[table_number]}"):
                    node.query(f"GRANT SELECT ON {table_names[table_number]} TO {target_name}")

                    tables_granted.append(f'{table_names[table_number]}')

        yield (', ').join(tables_granted)

    finally:
        for table_number in range(len(table_names)):
            with Finally(f"I revoke the select privilege on {table_names[table_number]}"):
                node.query(f"REVOKE SELECT ON {table_names[table_number]} FROM {target_name}")
