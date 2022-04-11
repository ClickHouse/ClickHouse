from lightweight_delete.tests.steps import *
from lightweight_delete.requirements import *
from lightweight_delete.tests.alter_after_delete import (
    delete_odd,
    alter_detach_partition,
    alter_attach_partition,
    alter_freeze_partition,
    alter_drop_partition,
    alter_add_column,
    alter_drop_column,
    alter_modify_column,
    alter_clear_column,
    alter_update_in_partition,
)


@TestStep
def random_alter_operations(
    self, table_name, alter_commands, iterations=40, delay=0.05
):
    """Run random operations from alter_commands in random order in loop."""
    for i in range(iterations):
        random_command = random.choice(alter_commands)
        random_command(table_name=table_name)
        time.sleep(delay)


@TestScenario
def random_concurrent_alter(self, node=None):
    """Check that clickhouse supports concurrent deletes with alter operations
    when random alter operations perform in ransom order.
    """
    alter_commands = [
        alter_detach_partition,
        alter_attach_partition,
        alter_freeze_partition,
        alter_drop_partition,
        alter_add_column,
        alter_drop_column,
        alter_modify_column,
        alter_clear_column,
        alter_update_in_partition,
    ]

    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_partitioned_table(table_name=table_name)

    with When(
        "I insert a lot of data into the table",
        description="100 partitions 1 part in each block_size=100",
    ):
        insert(
            table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
        )

    with Then(
        "I perform concurrent operations",
        description="deleting odd rows and random concurrent alter operations",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(num_partitions=100, table_name=table_name)
        Step(
            name="random alter operations", test=random_alter_operations, parallel=True
        )(table_name=table_name, alter_commands=alter_commands)


@TestFeature
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Compatibility_ConcurrentOperations("1.0"),
)
@Name("random concurrent alter")
def feature(self, node="clickhouse1"):
    """Check that clickhouse supports concurrent deletes with alter operations."""

    if self.context.use_alter_delete:
        xfail(
            "alter delete does not support concurrent mutations",
            reason="alter delete does not support concurrent mutations",
        )

    self.context.node = self.context.cluster.node(node)
    self.context.table_engine = "MergeTree"
    for scenario in loads(current_module(), Scenario):
        scenario()
