from testflows.core import *
from testflows.asserts import error

from example.requirements import *


@TestScenario
@Name("select 1")
@Requirements(RQ_SRS_001_Example_Select_1("1.0"))
def scenario(self, node="clickhouse1"):
    """Check that ClickHouse returns 1 when user executes `SELECT 1` query."""
    node = self.context.cluster.node(node)

    with When("I execute query select 1"):
        r = node.query("SELECT 1").output.strip()

    with Then("the result should be 1"):
        assert r == "1", error()
