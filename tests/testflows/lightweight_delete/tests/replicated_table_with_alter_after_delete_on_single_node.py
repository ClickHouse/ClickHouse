from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *
import random
import time


@TestScenario
def detach_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that detach partition after delete perform correctly with replicated table on single node."""

    table_name = getuid()

    with Given("I have replicated table"):
        create_table(table_name=table_name)

    with When("I insert data into the table"):
        insert_replicated(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then(
        "I specify that delete and alter operations perform on the clickhouse1 node"
    ):
        self.context.node = self.context.cluster.node("clickhouse1")

    with Then("I check detach partition perform correctly"):
        detach_partition_after_delete_in_the_table(
            table_name=table_name, nodes=["clickhouse1", "clickhouse2", "clickhouse3"]
        )


@TestScenario
def drop_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that drop partition after delete perform correctly with replicated table on single node."""
    table_name = getuid()

    with Given("I have replicated table"):
        create_table(table_name=table_name)

    with When("I insert data into the table"):
        insert_replicated(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then(
        "I specify that delete and alter operations perform on the clickhouse1 node"
    ):
        self.context.node = self.context.cluster.node("clickhouse1")

    with Then("I check drop partition perform correctly"):
        drop_partition_after_delete_in_the_table(
            table_name=table_name, nodes=["clickhouse1", "clickhouse2", "clickhouse3"]
        )


@TestScenario
def attach_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that attach partition after delete perform correctly with replicated table on single node."""
    table_name = getuid()

    with Given("I have replicated table"):
        create_table(table_name=table_name)

    with When("I insert data into the table"):
        insert_replicated(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then(
        "I specify that delete and alter operations perform on the clickhouse1 node"
    ):
        self.context.node = self.context.cluster.node("clickhouse1")

    with Then("I check attach partition perform correctly"):
        attach_partition_after_delete_in_the_table(
            table_name=table_name, nodes=["clickhouse1", "clickhouse2", "clickhouse3"]
        )


@TestScenario
def freeze_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that freeze partition after delete perform correctly with replicated table on single node."""
    table_name = getuid()

    with Given("I have replicated table"):
        create_table(table_name=table_name)

    with When("I insert data into the table"):
        insert_replicated(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then(
        "I specify that delete and alter operations perform on the clickhouse1 node"
    ):
        self.context.node = self.context.cluster.node("clickhouse1")

    with Then("I check freeze partition perform correctly"):
        freeze_partition_after_delete_in_the_table(
            table_name=table_name, nodes=["clickhouse1", "clickhouse2", "clickhouse3"]
        )


@TestScenario
def unfreeze_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that unfreeze partition after delete perform correctly with replicated table on single node."""
    table_name = getuid()

    with Given("I have replicated table"):
        create_table(table_name=table_name)

    with When("I insert data into the table"):
        insert_replicated(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then(
        "I specify that delete and alter operations perform on the clickhouse1 node"
    ):
        self.context.node = self.context.cluster.node("clickhouse1")

    with Then("I check unfreeze partition perform correctly"):
        unfreeze_partition_after_delete_in_the_table(
            table_name=table_name, nodes=["clickhouse1", "clickhouse2", "clickhouse3"]
        )


@TestScenario
def add_column_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that add column after delete perform correctly with replicated table on single node."""
    table_name = getuid()

    with Given("I have replicated table"):
        create_table(table_name=table_name)

    with When("I insert data into the table"):
        insert_replicated(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then(
        "I specify that delete and alter operations perform on the clickhouse1 node"
    ):
        self.context.node = self.context.cluster.node("clickhouse1")

    with Then("I check add column perform correctly"):
        add_column_after_delete_in_the_table(
            table_name=table_name,
            partitions=partitions,
            nodes=["clickhouse1", "clickhouse2", "clickhouse3"],
        )


@TestScenario
def drop_column_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that drop column after delete perform correctly with replicated table on single node."""
    table_name = getuid()

    with Given("I have replicated table"):
        create_table(table_name=table_name)

    with When("I insert data into the table"):
        insert_replicated(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then(
        "I specify that delete and alter operations perform on the clickhouse1 node"
    ):
        self.context.node = self.context.cluster.node("clickhouse1")

    with Then("I check drop column perform correctly"):
        drop_column_after_delete_in_the_table(
            table_name=table_name, nodes=["clickhouse1", "clickhouse2", "clickhouse3"]
        )


@TestScenario
def clear_column_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that clear column after delete perform correctly with replicated table on single node."""
    table_name = getuid()

    with Given("I have replicated table"):
        create_table(table_name=table_name)

    with When("I insert data into the table"):
        insert_replicated(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then(
        "I specify that delete and alter operations perform on the clickhouse1 node"
    ):
        self.context.node = self.context.cluster.node("clickhouse1")

    with Then("I check clear column perform correctly"):
        add_column_after_delete_in_the_table(
            table_name=table_name, nodes=["clickhouse1", "clickhouse2", "clickhouse3"]
        )


@TestScenario
def modify_column_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that modify column after delete perform correctly with replicated table on single node."""
    table_name = getuid()

    with Given("I have replicated table"):
        create_table(table_name=table_name)

    with When("I insert data into the table"):
        insert_replicated(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then(
        "I specify that delete and alter operations perform on the clickhouse1 node"
    ):
        self.context.node = self.context.cluster.node("clickhouse1")

    with Then("I check modify column perform correctly"):
        modify_column_after_delete_in_the_table(
            table_name=table_name, nodes=["clickhouse1", "clickhouse2", "clickhouse3"]
        )


@TestScenario
def comment_column_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that comment column after delete perform correctly with replicated table on single node."""
    table_name = getuid()

    with Given("I have replicated table"):
        create_table(table_name=table_name)

    with When("I insert data into the table"):
        insert_replicated(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then(
        "I specify that delete and alter operations perform on the clickhouse1 node"
    ):
        self.context.node = self.context.cluster.node("clickhouse1")

    with Then("I check comment column perform correctly"):
        comment_column_after_delete_in_the_table(
            table_name=table_name,
            partitions=partitions,
            nodes=["clickhouse1", "clickhouse2", "clickhouse3"],
        )


@TestScenario
def fetch_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that fetch partition after delete perform correctly with replicated table on single node."""
    table_name_1 = getuid() + "_1"
    table_name_2 = getuid() + "_2"

    with Given("I have two replicated tables"):
        create_table(table_name=table_name_1)
        create_table(table_name=table_name_2)

    with When("I insert data into  tables"):
        insert_replicated(
            table_name=table_name_1,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )
        insert_replicated(
            table_name=table_name_2,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with When(
        "I specify that delete and alter operations perform on the clickhouse1 node"
    ):
        node = self.context.node = self.context.cluster.node("clickhouse1")

    with When("I compute expected output"):
        output11 = node.query(f"SELECT count(*) FROM {table_name_1}").output
        output12 = node.query(
            f"SELECT count(*) FROM {table_name_2} WHERE NOT(x % 2 == 0 or id != 3)"
        ).output
        output2 = node.query(
            f"SELECT count(*) FROM {table_name_2} WHERE NOT(x % 2 == 0)"
        ).output

    with And("I delete half of the first table"):
        delete_odd(num_partitions=partitions, table_name=table_name_2)

    with Then("I fetch partition from table 1 to table 2"):
        alter_fetch_partition(
            table_name=table_name_1,
            partition_expr="3",
            path=f"/clickhouse/tables/shard0/{table_name_2}",
        )
        alter_attach_partition(table_name=table_name_1, partition_expr="3")

    with Then("I check result"):
        check_query_on_all_nodes(
            query=f"SELECT count(*) FROM {table_name_1}",
            output=str(int(output11) + int(output12)),
        )
        check_query_on_all_nodes(
            query=f"SELECT count(*) FROM {table_name_2}", output=output2
        )


@TestFeature
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_EventualConsistency("1.0"))
@Name("replicated table with alter after delete on single node")
def feature(self):
    """Check that delete operations replicate in an eventually consistent manner between replicas."""

    for table_engine in [
        "ReplicatedMergeTree",
        "ReplicatedReplacingMergeTree",
        "ReplicatedSummingMergeTree",
        "ReplicatedAggregatingMergeTree",
        "ReplicatedCollapsingMergeTree",
        "ReplicatedVersionedCollapsingMergeTree",
        "ReplicatedGraphiteMergeTree",
    ]:

        with Feature(f"{table_engine}"):
            self.context.table_engine = table_engine
            for scenario in loads(current_module(), Scenario):
                scenario()
