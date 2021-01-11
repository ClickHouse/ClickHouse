from contextlib import contextmanager

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Table_PublicTables("1.0"),
)
def public_tables(self, node=None):
    """Check that a user with no privilege is able to select from public tables.
    """
    user_name = f"user_{getuid()}"
    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        with Then("I check the user is able to select on system.one"):
            node.query("SELECT count(*) FROM system.one", settings = [("user",user_name)])

        with And("I check the user is able to select on system.numbers"):
            node.query("SELECT * FROM system.numbers LIMIT 1", settings = [("user",user_name)])

        with And("I check the user is able to select on system.contributors"):
            node.query("SELECT count(*) FROM system.contributors", settings = [("user",user_name)])

        with And("I check the user is able to select on system.functions"):
            node.query("SELECT count(*) FROM system.functions", settings = [("user",user_name)])

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Table_QueryLog("1.0"),
)
def query_log(self, node=None):
    """Check that a user with no privilege is only able to see their own queries.
    """
    user_name = f"user_{getuid()}"
    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        with Given("I create a query"):
            node.query("SELECT 1")

        with Then("The user reads system.query_log"):
            output = node.query("SELECT count() FROM system.query_log", settings = [("user",user_name)]).output
            assert output == 0, error()

@TestFeature
@Name("public tables")
def feature(self, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    Scenario(run=public_tables, setup=instrument_clickhouse_server_log, flags=TE)
    Scenario(run=query_log, setup=instrument_clickhouse_server_log, flags=TE)