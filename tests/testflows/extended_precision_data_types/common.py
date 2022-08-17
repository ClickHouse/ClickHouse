import uuid

from contextlib import contextmanager

from testflows.core.name import basename, parentname
from testflows._core.testtype import TestSubType
from testflows.asserts import values, error, snapshot

from helpers.common import *

rounding_precision = 7

@contextmanager
def allow_experimental_bigint(node):
    """Enable experimental big int setting in Clickhouse.
    """
    setting = ("allow_experimental_bigint_types", 1)
    default_query_settings = None

    try:
        with Given("I add allow_experimental_bigint to the default query settings"):
            default_query_settings = getsattr(current().context, "default_query_settings", [])
            default_query_settings.append(setting)
        yield
    finally:
        with Finally("I remove allow_experimental_bigint from the default query settings"):
            if default_query_settings:
                try:
                    default_query_settings.pop(default_query_settings.index(setting))
                except ValueError:
                    pass

@TestStep(Given)
def allow_experimental_map_type(self):
    """Set allow_experimental_map_type = 1
    """
    setting = ("allow_experimental_map_type", 1)
    default_query_settings = None

    try:
        with By("adding allow_experimental_map_type to the default query settings"):
            default_query_settings = getsattr(current().context, "default_query_settings", [])
            default_query_settings.append(setting)
        yield
    finally:
        with Finally("I remove allow_experimental_map_type from the default query settings"):
            if default_query_settings:
                try:
                    default_query_settings.pop(default_query_settings.index(setting))
                except ValueError:
                    pass

def execute_query(sql, expected=None, format="TabSeparatedWithNames", compare_func=None):
    """Execute SQL query and compare the output to the snapshot.
    """
    name = basename(current().name)

    with When("I execute query", description=sql):
        r = current().context.node.query(sql + " FORMAT " + format)

    if expected is not None:
        with Then("I check output against expected"):

            if compare_func is None:
                assert r.output.strip() == expected, error()

            else:
                assert compare_func(r.output.strip(), expected), error()

    else:
        with Then("I check output against snapshot"):
            with values() as that:
                assert that(snapshot("\n" + r.output.strip() + "\n", "tests", name=name, encoder=str)), error()

@TestStep(Given)
def table(self, data_type, name="table0"):
    """Create a table.
    """
    node = current().context.node

    try:
        with By("creating table"):
            node.query(f"CREATE TABLE {name}(a {data_type}) ENGINE = Memory")
        yield

    finally:
        with Finally("drop the table"):
            node.query(f"DROP TABLE IF EXISTS {name}")

def getuid():
    """Create a unique variable name based on the test it is called from.
    """
    if current().subtype == TestSubType.Example:
        testname = f"{basename(parentname(current().name)).replace(' ', '_').replace(',','')}"
    else:
        testname = f"{basename(current().name).replace(' ', '_').replace(',','')}"

    for char in ['(', ')', '[', ']','\'']:
        testname = testname.replace(f'{char}', '')

    return testname + "_" + str(uuid.uuid1()).replace('-', '_')

def to_data_type(data_type, value):
    """Return a conversion statement based on the data type provided
    """
    if data_type in ['Decimal256(0)']:
        return f'toDecimal256(\'{value}\',0)'

    else:
        return f'to{data_type}(\'{value}\')'


data_types = [
    ('Int128', '-170141183460469231731687303715884105728', '170141183460469231731687303715884105727'),
    ('Int256', '-57896044618658097711785492504343953926634992332820282019728792003956564819968', '57896044618658097711785492504343953926634992332820282019728792003956564819967'),
    ('UInt128','0','340282366920938463463374607431768211455'),
    ('UInt256', '0', '115792089237316195423570985008687907853269984665640564039457584007913129639935'),
]

Decimal256_min_max = -1000000000000000000000000000000000000000000000000000000000000000000000000000,1000000000000000000000000000000000000000000000000000000000000000000000000000
