from lightweight_delete.tests.steps import *
from lightweight_delete.requirements import *
from lightweight_delete.tests.alter_after_delete import (
    delete_odd,
    alter_freeze_partition,
    alter_drop_partition,
    alter_add_column,
)


@TestScenario
def concurrent_delete_attach_detach_partition(self, node=None):
    """Check that concurrent delete and attach detach partition perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert a lot of data into the table",
        description="10 partitions 1 part block_size=100",
    ):
        insert(
            table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
        )

    with When("I compute expected output"):
        output1 = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with When("I compute expected output"):
        output2 = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0 and id != 3)"
        ).output

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and detach the third partition",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(num_partitions=10, table_name=table_name)
        Step(name="attach detach in a loop", test=attach_detach_in_loop, parallel=True)(
            table_name=table_name, partition_expr="3", node=node
        )

    with Then(
        "I check that rows are deleted",
        description="rows can be not deleted in detached partition",
    ):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output in (output1, output2), error()


@TestScenario
def concurrent_delete_drop_partition(self, node=None):
    """Check that concurrent delete and drop partition perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert a lot of data into the table",
        description="10 partitions 1 part block_size=100",
    ):
        insert(
            table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
        )

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0 or id = 3)"
        ).output

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and drop the third partition",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(num_partitions=10, table_name=table_name)
        Step(name="drop partition", test=alter_drop_partition, parallel=True)(
            table_name=table_name, partition_expr="3", node=node
        )

    with Then(
        "I check that rows are deleted",
        description="50 rows in reach partition, 9 partitions",
    ):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == output, error()


@TestScenario
def concurrent_delete_freeze_partition(self, node=None):
    """Check that concurrent delete and freeze partition perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert a lot of data into the table",
        description="10 partitions 1 part block_size=100",
    ):
        insert(
            table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
        )

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and freeze the third partition",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(num_partitions=10, table_name=table_name)
        Step(name="freeze partition", test=alter_freeze_partition, parallel=True)(
            table_name=table_name, partition_expr="3", node=node
        )

    with Then(
        "I check that rows are deleted",
        description="50 rows in reach partition, 10 partitions",
    ):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == output, error()


@TestScenario
def concurrent_add_drop_column_and_delete(self, node=None):
    """Check that concurrent delete and add drop column perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert a lot of data into the table",
        description="10 partitions 1 part block_size=100",
    ):
        insert(
            table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
        )

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and add drop column",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(num_partitions=10, table_name=table_name)
        Step(name="add drop column", test=add_drop_column_in_loop, parallel=True)(
            table_name=table_name,
            column_name="qkrq",
            column_type="Int32",
            default_expr="DEFAULT 777",
            node=node,
        )

    with Then(
        "I check that rows are deleted",
        description="50 rows in reach partition, 10 partitions",
    ):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == output, error()


@TestScenario
def concurrent_modify_column_and_delete(self, node=None):
    """Check that concurrent delete and add drop column perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert a lot of data into the table",
        description="10 partitions 1 part block_size=100",
    ):
        insert(
            table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
        )

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with And("I add column to modify it in the loop"):
        alter_add_column(
            table_name=table_name,
            column_name="qkrq",
            column_type="Int32",
            default_expr="DEFAULT 555",
        )

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and modify column",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(num_partitions=10, table_name=table_name)
        Step(name="modify column", test=modify_column_in_loop, parallel=True)(
            table_name=table_name, column_name="qkrq", node=node
        )

    with Then(
        "I check that rows are deleted",
        description="50 rows in reach partition, 10 partitions",
    ):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == output, error()


@TestScenario
def concurrent_clear_update_and_delete(self, node=None):
    """Check that concurrent clear column update column and delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert a lot of data into the table",
        description="100 partitions 1 part block_size=100",
    ):
        insert(
            table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
        )

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with And("I add column to modify it in the loop"):
        alter_add_column(table_name=table_name, column_name="qkrq", column_type="Int32")

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and clear and update column",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(num_partitions=10, table_name=table_name)
        Step(name="clear update column", test=clear_update_in_loop, parallel=True)(
            table_name=table_name, column_name="qkrq", node=node
        )

    with Then("I check that rows are deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == output, error()


@TestFeature
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Compatibility_ConcurrentOperations("1.0"),
)
@Name("concurrent alter and delete")
def feature(self, node="clickhouse1"):
    """Check that clickhouse supports concurrent deletes with alter operations."""
    self.context.node = self.context.cluster.node(node)

    if self.context.use_alter_delete:
        xfail(
            reason="alter delete does not support concurrent mutations",
        )

    for table_engine in [
        "MergeTree",
        "ReplacingMergeTree",
        "SummingMergeTree",
        "AggregatingMergeTree",
        "CollapsingMergeTree",
        "VersionedCollapsingMergeTree",
        "GraphiteMergeTree",
    ]:

        with Feature(f"{table_engine}"):
            self.context.table_engine = table_engine
            for scenario in loads(current_module(), Scenario):
                scenario()
