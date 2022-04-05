from testflows.core import *
from testflows.asserts import error


@TestScenario
def check_non_secure_connection(self, node=None):
    """Check that non-secure connection can be made
    to the server.
    """
    if node is None:
        node = self.context.node

    with When("I try to execute query using non-secure connection"):
        r = node.query("SELECT 1")

    with Then("it should work"):
        assert r.output == "1", error()


@TestFeature
@Name("sanity")
def feature(self, node="clickhouse1"):
    """Sanity check suite."""
    self.context.node = self.context.cluster.node(node)

    Scenario(run=check_non_secure_connection)
