from lightweight_delete.tests.steps import *
from lightweight_delete.requirements import *


@TestStep
def insert_in_loop(
    self,
    table_name,
    partitions=10,
    parts_per_partition=10,
    block_size=50,
    delay=0.1,
    iterations=10,
    node=None,
):
    """Inserting the same data in loop."""

    if node is None:
        node = self.context.node

    for i in range(iterations):
        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

        time.sleep(delay)


@TestStep
def delete_in_loop(self, table_name, condition, delay=0.1, iterations=10, node=None):
    """Deleting the same data in loop."""
    if node is None:
        node = self.context.node

    for i in range(iterations):
        delete(table_name=table_name, condition=condition)
        time.sleep(delay)


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Compatibility_ConcurrentInserts_DeletesOnManyParts(
        "1.0"
    ),
)
def concurrent_insert_delete_many_parts(self, node=None):
    """Check that clickhouse support executing INSERT and DELETE statements concurrently
    when INSERT creates many parts.
    """
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

    with Then("I perform concurrent operations"):
        Step(name="insert rows in loop", test=insert_in_loop, parallel=True)(
            table_name=table_name, node=node
        )
        Step(name="delete in loop", test=delete_in_loop, parallel=True)(
            table_name=table_name, condition="WHERE x < 25", node=node
        )

    with Then("I check that rows are deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert int(r.output) > 0, error()


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Compatibility_ConcurrentInserts_DeletesOfTheSameData(
        "1.0"
    ),
)
def concurrent_insert_delete_the_same_data(self, node=None):
    """Check that clickhouse support executing INSERT and DELETE statements concurrently
    on the same data.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert a lot of data into the table",
        description="10 partitions, 1 part, block_size=100",
    ):
        insert(
            table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
        )

    with Then("I perform concurrent operations"):
        Step(name="insert rows in loop", test=insert_in_loop, parallel=True)(
            partitions=1,
            parts_per_partition=1,
            block_size=25,
            table_name=table_name,
            node=node,
        )
        Step(name="delete in loop", test=delete_in_loop, parallel=True)(
            table_name=table_name, condition="WHERE id = 0 AND x < 25", node=node
        )

    with Then("I check that rows are deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert int(r.output) > 0, error()


@TestFeature
@Name("compatibility")
def feature(self, node="clickhouse1"):
    """Check that clickhouse support executing INSERT and DELETE statements concurrently."""
    self.context.node = self.context.cluster.node(node)

    self.context.table_engine = "MergeTree"

    for scenario in loads(current_module(), Scenario):
        scenario()
