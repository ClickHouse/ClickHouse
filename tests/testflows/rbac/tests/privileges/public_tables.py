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

        with When("I check the user is able to select on system.one"):
            node.query("SELECT count(*) FROM system.one", settings = [("user",user_name)])

        with And("I check the user is able to select on system.numbers"):
            node.query("SELECT * FROM system.numbers LIMIT 1", settings = [("user",user_name)])

        with And("I check the user is able to select on system.contributors"):
            node.query("SELECT count(*) FROM system.contributors", settings = [("user",user_name)])

        with And("I check the user is able to select on system.functions"):
            node.query("SELECT count(*) FROM system.functions", settings = [("user",user_name)])

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Table_SensitiveTables("1.0"),
)
def sensitive_tables(self, node=None):
    """Check that a user with no privilege is not able to see from these tables.
    """
    user_name = f"user_{getuid()}"
    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        with Given("I create a query"):
            node.query("SELECT 1")

        with When("I select from processes"):
            output = node.query("SELECT count(*) FROM system.processes", settings = [("user",user_name)]).output
            assert output == 0, error()

        with And("I select from query_log"):
            output = node.query("SELECT count(*) FROM system.query_log", settings = [("user",user_name)]).output
            assert output == 0, error()

        with And("I select from query_thread_log"):
            output = node.query("SELECT count(*) FROM system.query_thread_log", settings = [("user",user_name)]).output
            assert output == 0, error()

        with And("I select from clusters"):
            output = node.query("SELECT count(*) FROM system.clusters", settings = [("user",user_name)]).output
            assert output == 0, error()

        with And("I select from events"):
            output = node.query("SELECT count(*) FROM system.events", settings = [("user",user_name)]).output
            assert output == 0, error()

        with And("I select from graphite_retentions"):
            output = node.query("SELECT count(*) FROM system.graphite_retentions", settings = [("user",user_name)]).output
            assert output == 0, error()

        with And("I select from stack_trace"):
            output = node.query("SELECT count(*) FROM system.stack_trace", settings = [("user",user_name)]).output
            assert output == 0, error()

        with And("I select from trace_log"):
            output = node.query("SELECT count(*) FROM system.trace_log", settings = [("user",user_name)]).output
            assert output == 0, error()

        with And("I select from user_directories"):
            output = node.query("SELECT count(*) FROM system.user_directories", settings = [("user",user_name)]).output
            assert output == 0, error()

        with And("I select from zookeeper"):
            output = node.query("SELECT count(*) FROM system.zookeeper WHERE path = '/clickhouse' ", settings = [("user",user_name)]).output
            assert output == 0, error()

        with And("I select from macros"):
            output = node.query("SELECT count(*) FROM system.macros", settings = [("user",user_name)]).output
            assert output == 0, error()

@TestFeature
@Name("public tables")
def feature(self, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    Scenario(run=public_tables, setup=instrument_clickhouse_server_log)
    Scenario(run=sensitive_tables, setup=instrument_clickhouse_server_log)
