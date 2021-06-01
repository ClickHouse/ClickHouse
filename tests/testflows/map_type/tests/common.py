import uuid
from collections import namedtuple

from testflows.core import *
from testflows.core.name import basename, parentname
from testflows._core.testtype import TestSubType

def getuid():
    if current().subtype == TestSubType.Example:
        testname = f"{basename(parentname(current().name)).replace(' ', '_').replace(',','')}"
    else:
        testname = f"{basename(current().name).replace(' ', '_').replace(',','')}"
    return testname + "_" + str(uuid.uuid1()).replace('-', '_')

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

@TestStep(Given)
def create_table(self, name, statement, on_cluster=False):
    """Create table.
    """
    node = current().context.node
    try:
        with Given(f"I have a {name} table"):
            node.query(statement.format(name=name))
        yield name
    finally:
        with Finally("I drop the table"):
            if on_cluster:
                node.query(f"DROP TABLE IF EXISTS {name} ON CLUSTER {on_cluster}")
            else:
                node.query(f"DROP TABLE IF EXISTS {name}")
