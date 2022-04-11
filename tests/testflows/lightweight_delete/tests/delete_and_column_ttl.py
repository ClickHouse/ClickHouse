from lightweight_delete.tests.steps import *
from lightweight_delete.requirements import *
from lightweight_delete.tests.alter_after_delete import alter_update_in_partition


@TestScenario
def delete_after_column_ttl(self, node=None):
    """Check that delete statement after column ttl perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_partitioned_table(
            table_name=table_name,
            extra_table_col=", d DateTime, a Int DEFAULT 777 TTL d + INTERVAL 1 HOUR",
        )

    with When("I insert data into the table"):
        now = time.time()
        wait_expire = 31 * 60
        date = now
        for j in range(100):
            for i in range(6):
                values = f"({i}, {i}, toDateTime({date-i*wait_expire}), {i})"
                node.query(f"INSERT INTO {table_name} VALUES {values}")

    with And("I force merges"):
        optimize_table(table_name=table_name)

    with And("I delete odd rows in the table"):
        delete(table_name=table_name, condition="WHERE a % 2 = 0")

    with Then(
        "I expect rows are deleted", description="600 rows 100 should be deleted"
    ):
        r = node.query(f"SELECT count(*) from {table_name}")
        assert r.output == str(500), error()


@TestScenario
def delete_and_column_ttl_concurrent(self, node=None):
    """Check that concurrent lightweight delete and column ttl perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_partitioned_table(
            table_name=table_name,
            extra_table_col=", d DateTime, a Int DEFAULT 777 TTL d + INTERVAL 1 HOUR",
        )

    with When("I insert data into the table"):
        now = time.time()
        wait_expire = 31 * 60
        date = now
        for j in range(100):
            for i in range(6):
                values = f"({i}, {i}, toDateTime({date-i*wait_expire}), {i})"
                node.query(f"INSERT INTO {table_name} VALUES {values}")

    with And("I delete odd rows in the table"):
        delete(table_name=table_name, condition="WHERE a % 2 = 0")

    with Then(
        "I expect rows are deleted", description="600 rows half of them may be deleted"
    ):
        r = node.query(f"SELECT count(*) from {table_name}")
        assert int(r.output) >= 300, error()


@TestScenario
def column_ttl_after_delete(self, node=None):
    """Check that column ttl after delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_partitioned_table(
            table_name=table_name,
            extra_table_col=", d DateTime, a Int DEFAULT 777 TTL d + INTERVAL 1 HOUR",
        )

    with When("I insert data into the table"):
        now = time.time()
        wait_expire = 31 * 60
        date = now
        for j in range(10):
            for i in range(6):
                values = f"({i}, {i}, toDateTime({date}), {i})"
                node.query(f"INSERT INTO {table_name} VALUES {values}")

    with And("I delete odd rows in the table"):
        delete(table_name=table_name, condition="WHERE a % 2 = 0")

    with And("I update table to make column ttl work"):
        alter_update_in_partition(
            table_name=table_name,
            update_expr=f"d = toDateTime({date - wait_expire*10})",
        )

    with And("I force merges"):
        optimize_table(table_name=table_name)

    with Then(
        "I expect rows are deleted",
        description="600 rows half of them should be deleted",
    ):
        r = node.query(f"SELECT count() FROM {table_name} WHERE a = 777 ")
        assert int(r.output) == 30, error()


@TestFeature
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_ColumnTTL("1.0"))
@Name("delete and column ttl")
def feature(self, node="clickhouse1"):
    """Check that delete and column ttl together perform correctly."""
    self.context.node = self.context.cluster.node(node)
    self.context.table_engine = "MergeTree"

    for scenario in loads(current_module(), Scenario):
        scenario()
