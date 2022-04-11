from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
@Requirements()
def concurrent_delete_attach_detach_partition_in_replicated_table_on_single_node(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that concurrent delete detach partition and attach partition perform
    correctly with replicated table on single node.
    """
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name,
        partitions=partitions,
        parts_per_partition=parts_per_partition,
        block_size=block_size,
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output1 = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 = 0)"
        ).output
        expected_output2 = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 = 0 AND id != 3)"
        ).output

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and attach detach the third partition",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(
            num_partitions=partitions,
            table_name=table_name,
            node=self.context.cluster.node("clickhouse1"),
        )
        Step(name="attach detach in a loop", test=attach_detach_in_loop, parallel=True)(
            table_name=table_name,
            partition_expr="3",
            node=self.context.cluster.node("clickhouse1"),
        )

    with Then("I check concurrent operations perform correctly"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                for attempt in retries(timeout=30, delay=1):
                    with attempt:
                        r = node.query(f"SELECT count(*) FROM {table_name}")
                        assert r.output in (expected_output1, expected_output2), error()


@TestScenario
@Requirements()
def concurrent_add_drop_column_and_delete_in_replicated_table_on_single_node(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that concurrent delete and add drop column perform
    correctly with replicated table on single node.
    """
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name,
        partitions=partitions,
        parts_per_partition=parts_per_partition,
        block_size=block_size,
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 = 0)"
        ).output

    alter_add_column(
        table_name=table_name,
        column_name="qkrq",
        column_type="Int32",
        node=self.context.cluster.node("clickhouse1"),
    )

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and add drop column",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(
            num_partitions=partitions,
            table_name=table_name,
            node=self.context.cluster.node("clickhouse1"),
        )
        Step(name="add drop column", test=add_drop_column_in_loop, parallel=True)(
            table_name=table_name,
            column_name="qkrq",
            column_type="Int32",
            default_expr="DEFAULT 777",
            node=self.context.cluster.node("clickhouse1"),
        )

    check_query_on_all_nodes(
        query=f"SELECT count(*) FROM {table_name}", output=expected_output
    )


@TestScenario
@Requirements()
def concurrent_modify_column_and_delete_in_replicated_table_on_single_node(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that concurrent delete and modify column perform
    correctly with replicated table on single node.
    """
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name,
        partitions=partitions,
        parts_per_partition=parts_per_partition,
        block_size=block_size,
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 = 0)"
        ).output

    alter_add_column(
        table_name=table_name,
        column_name="qkrq",
        column_type="Int32",
        node=self.context.cluster.node("clickhouse1"),
    )

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and modify delete column",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(
            num_partitions=partitions,
            table_name=table_name,
            node=self.context.cluster.node("clickhouse1"),
        )
        Step(name="modify column", test=modify_column_in_loop, parallel=True)(
            table_name=table_name,
            column_name="qkrq",
            node=self.context.cluster.node("clickhouse1"),
        )

    check_query_on_all_nodes(
        query=f"SELECT count(*) FROM {table_name}", output=expected_output
    )


@TestScenario
@Requirements()
def concurrent_clear_update_and_delete_in_replicated_table_on_single_node(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that concurrent delete and clear update+ column perform
    correctly with replicated table on single node.
    """
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name,
        partitions=partitions,
        parts_per_partition=parts_per_partition,
        block_size=block_size,
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 = 0)"
        ).output

    alter_add_column(
        table_name=table_name,
        column_name="qkrq",
        column_type="Int32",
        node=self.context.cluster.node("clickhouse1"),
    )

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and clear update column",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(
            num_partitions=partitions,
            table_name=table_name,
            node=self.context.cluster.node("clickhouse1"),
        )
        Step(name="clear update column", test=clear_update_in_loop, parallel=True)(
            table_name=table_name,
            column_name="qkrq",
            node=self.context.cluster.node("clickhouse1"),
        )

    check_query_on_all_nodes(
        query=f"SELECT count(*) FROM {table_name}", output=expected_output
    )


@TestScenario
@Requirements()
def concurrent_delete_attach_detach_partition_in_replicated_table_on_two_nodes(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that concurrent delete detach partition and attach partition perform
    correctly with replicated table on two nodes.
    """
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name,
        partitions=partitions,
        parts_per_partition=parts_per_partition,
        block_size=block_size,
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output1 = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 = 0)"
        ).output
        expected_output2 = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 = 0 AND id != 3)"
        ).output

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and attach detach the third partition",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(
            num_partitions=partitions,
            table_name=table_name,
            node=self.context.cluster.node("clickhouse1"),
        )
        Step(name="attach detach in a loop", test=attach_detach_in_loop, parallel=True)(
            table_name=table_name,
            partition_expr="3",
            node=self.context.cluster.node("clickhouse2"),
        )

    with Then("I check concurrent operations perform correctly"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                for attempt in retries(timeout=30, delay=1):
                    with attempt:
                        node.query(f"select * from {table_name} order by id,x")
                        r = node.query(f"SELECT count(*) FROM {table_name}")
                        assert r.output in (expected_output1, expected_output2), error()


@TestScenario
@Requirements()
def concurrent_add_drop_column_and_delete_in_replicated_table_on_two_nodes(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that concurrent delete and add drop column perform
    correctly with replicated table on two nodes.
    """
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name,
        partitions=partitions,
        parts_per_partition=parts_per_partition,
        block_size=block_size,
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 = 0)"
        ).output

    alter_add_column(
        table_name=table_name,
        column_name="qkrq",
        column_type="Int32",
        node=self.context.cluster.node("clickhouse1"),
    )

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and add drop column",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(
            num_partitions=partitions,
            table_name=table_name,
            node=self.context.cluster.node("clickhouse1"),
        )
        Step(name="add drop column", test=add_drop_column_in_loop, parallel=True)(
            table_name=table_name,
            column_name="qkrq",
            column_type="Int32",
            default_expr="DEFAULT 777",
            node=self.context.cluster.node("clickhouse2"),
        )

    check_query_on_all_nodes(
        query=f"SELECT count(*) FROM {table_name}", output=expected_output
    )


@TestScenario
@Requirements()
def concurrent_modify_column_and_delete_in_replicated_table_on_two_nodes(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that concurrent delete and modify column perform
    correctly with replicated table on two nodes.
    """
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name,
        partitions=partitions,
        parts_per_partition=parts_per_partition,
        block_size=block_size,
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 = 0)"
        ).output

    alter_add_column(
        table_name=table_name,
        column_name="qkrq",
        column_type="Int32",
        node=self.context.cluster.node("clickhouse1"),
    )

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and modify delete column",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(
            num_partitions=partitions,
            table_name=table_name,
            node=self.context.cluster.node("clickhouse1"),
        )
        Step(name="modify column", test=modify_column_in_loop, parallel=True)(
            table_name=table_name,
            column_name="qkrq",
            node=self.context.cluster.node("clickhouse2"),
        )

    check_query_on_all_nodes(
        query=f"SELECT count(*) FROM {table_name}", output=expected_output
    )


@TestScenario
@Requirements()
def concurrent_clear_update_and_delete_in_replicated_table_on_two_nodes(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that concurrent delete and clear update column perform
    correctly with replicated table on two nodes.
    """
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name,
        partitions=partitions,
        parts_per_partition=parts_per_partition,
        block_size=block_size,
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 = 0)"
        ).output

    with And("I add column to modify it in the loop"):
        alter_add_column(
            table_name=table_name,
            column_name="qkrq",
            column_type="Int32",
            node=self.context.cluster.node("clickhouse1"),
        )

    with Then(
        "I perform concurrent operations",
        description="delete odd rows and clear update column",
    ):
        Step(
            name="delete odd rows from all partitions", test=delete_odd, parallel=True
        )(
            num_partitions=partitions,
            table_name=table_name,
            node=self.context.cluster.node("clickhouse1"),
        )
        Step(name="clear update column", test=clear_update_in_loop, parallel=True)(
            table_name=table_name,
            column_name="qkrq",
            node=self.context.cluster.node("clickhouse2"),
        )

    check_query_on_all_nodes(
        query=f"SELECT count(*) FROM {table_name}", output=expected_output
    )


@TestFeature
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Compatibility_ConcurrentOperations("1.0")
)
@Name("replicated table with concurrent alter and delete")
def feature(self):
    """Check that delete operations replicate in an eventually consistent manner between replicas."""

    table_engine = "ReplicatedMergeTree"

    self.context.table_engine = table_engine
    for scenario in loads(current_module(), Scenario):
        scenario()
