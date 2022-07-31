import uuid
from collections import namedtuple

from testflows.core import *
from testflows.core.name import basename, parentname
from testflows._core.testtype import TestSubType


def getuid():
    if current().subtype == TestSubType.Example:
        testname = (
            f"{basename(parentname(current().name)).replace(' ', '_').replace(',','')}"
        )
    else:
        testname = f"{basename(current().name).replace(' ', '_').replace(',','')}"
    return testname + "_" + str(uuid.uuid1()).replace("-", "_")


@TestStep(Given)
def create_table(self, name, statement, on_cluster=False):
    """Create table."""
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
