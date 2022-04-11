from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *
import time


@TestScenario
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_NonDeterministicFunctions("1.0"))
def nondeterministic_function(self, node=None):
    """Check that clickhouse support delete statement when where clause contains
    nondeterministic function now().
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_partitioned_table(table_name=table_name, extra_table_col=", d DateTime")

    with When("I insert data into the table"):
        now = time.time()
        wait_expire = 31 * 60
        date = now
        for i in range(30):
            values = f"({i + 1}, {i + 1}, toDateTime({date - i * wait_expire}))"
            insert(table_name=table_name, values=values)

    with When(f"I delete rows from the table using nondeterministic function"):
        delete(
            table_name=table_name,
            condition=f"WHERE toInt64(d) < toInt64(now()) - {15*wait_expire}",
        )

    with Then("I check that rows are deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == "15", error()


@TestFeature
@Name("non deterministic functions")
def feature(self, node="clickhouse1"):
    """Check that clickhouse support delete statement when where clause contains
    nondeterministic function.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.table_engine = "MergeTree"
    for scenario in loads(current_module(), Scenario):
        scenario()
