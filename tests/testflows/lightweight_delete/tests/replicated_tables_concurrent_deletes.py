from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_EventualConsistency("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_RowsRemovedFromReplica("1.0"),
)
def replicated_table_deleting_with_without_overlap(
    self, partitions=10, parts_per_partition=1, block_size=100
):
    """Check that clickhouse support concurrent DELETE statement which is related to replicated table
    by creating replicated table and deleting some rows in the table on different nodes.
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
            f"SELECT count(*) FROM {table_name}"
            f" WHERE NOT((x % 2 == 0 AND id < 50) OR (x % 2 == 0 AND id < 75 AND id > 24))"
        ).output

    with Then("I perform concurrent deletes"):
        Step(name="delete odd rows from clickhouse1", test=delete, parallel=True)(
            table_name=table_name,
            condition="WHERE x % 2 == 0 AND id < 50",
            node=self.context.cluster.node("clickhouse1"),
        )

        Step(name="delete odd rows from clickhouse2", test=delete, parallel=True)(
            table_name=table_name,
            condition="WHERE x % 2 == 0 AND id < 75 AND id > 24",
            node=self.context.cluster.node("clickhouse2"),
        )
    for attempt in retries(delay=1, timeout=30):
        with attempt:
            check_query_on_all_nodes(
                query=f"SELECT count(*) FROM {table_name}", output=expected_output
            )


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_EventualConsistency("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_RowsRemovedFromReplica("1.0"),
)
def replicated_table_deleting_without_overlap_entire_table(
    self, partitions=10, parts_per_partition=1, block_size=100
):
    """Check that clickhouse support concurrent DELETE statement which is related to replicated table
    by creating replicated table and deleting all rows in the table on different nodes without overlap.
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
            f"SELECT count(*) FROM {table_name} WHERE NOT(x<33 OR (x>32 AND x<68) OR x>67)"
        ).output

    with Then("I perform concurrent deletes"):
        Step(name="delete from clickhouse1", test=delete, parallel=True)(
            table_name=table_name,
            condition="WHERE x < 33",
            node=self.context.cluster.node("clickhouse1"),
        )

        Step(name="delete from clickhouse2", test=delete, parallel=True)(
            table_name=table_name,
            condition="WHERE x > 32 AND x < 68",
            node=self.context.cluster.node("clickhouse2"),
        )

        Step(name="delete from clickhouse3", test=delete, parallel=True)(
            table_name=table_name,
            condition="WHERE x > 67",
            node=self.context.cluster.node("clickhouse3"),
        )
    for attempt in retries(delay=1, timeout=30):
        with attempt:
            check_query_on_all_nodes(
                query=f"SELECT count(*) FROM {table_name}", output=expected_output
            )


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_EventualConsistency("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_RowsRemovedFromReplica("1.0"),
)
def replicated_table_deleting_with_overlap_entire_table(
    self, partitions=10, parts_per_partition=1, block_size=100
):
    """Check that clickhouse support concurrent DELETE statement which is related to replicated table
    by creating replicated table and deleting all rows in the table on different nodes with overlap.
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
            f"SELECT count(*) FROM {table_name} WHERE NOT(x<50 OR (x>25 AND x<75) OR x>50)"
        ).output

    with Then("I perform concurrent deletes"):
        By(name="deleting from clickhouse1", test=delete, parallel=True)(
            table_name=table_name,
            condition="WHERE x < 50",
            node=self.context.cluster.node("clickhouse1"),
        )

        By(name="deleting from clickhouse2", test=delete, parallel=True)(
            table_name=table_name,
            condition="WHERE x > 25 AND x < 75",
            node=self.context.cluster.node("clickhouse2"),
        )

        By(name="deleting from clickhouse3", test=delete, parallel=True)(
            table_name=table_name,
            condition="WHERE x > 50",
            node=self.context.cluster.node("clickhouse3"),
        )

    for attempt in retries(delay=1, timeout=30):
        with attempt:
            check_query_on_all_nodes(
                query=f"SELECT count(*) FROM {table_name}", output=expected_output
            )


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_EventualConsistency("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_RowsRemovedFromReplica("1.0"),
)
def replicated_table_deleting_in_loop_with_without_overlap(
    self, partitions=10, parts_per_partition=1, block_size=100
):
    """Check that clickhouse support concurrent DELETE statement which is related to replicated table
    by creating replicated table and deleting some rows in the table by partitions on different nodes.
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
            f"SELECT count(*) FROM {table_name}"
            f" WHERE NOT((x % 2 == 0 AND id < 50) OR (x % 2 == 0 AND id < 75 AND id > 24))"
        ).output

    with Then("I perform concurrent deletes"):
        Step(
            name="delete odd rows from clickhouse1 by partitions",
            test=delete_in_loop,
            parallel=True,
        )(
            partitions=partitions,
            table_name=table_name,
            condition="WHERE x % 2 == 0 AND id < 50",
            node=self.context.cluster.node("clickhouse1"),
        )

        Step(
            name="delete odd rows from clickhouse2 by partitions",
            test=delete_in_loop,
            parallel=True,
        )(
            partitions=partitions,
            table_name=table_name,
            condition="WHERE x % 2 == 0 AND id < 75 AND id > 24",
            node=self.context.cluster.node("clickhouse2"),
        )

    for attempt in retries(delay=1, timeout=30):
        with attempt:
            check_query_on_all_nodes(
                query=f"SELECT count(*) FROM {table_name}", output=expected_output
            )


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_EventualConsistency("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_RowsRemovedFromReplica("1.0"),
)
def replicated_table_deleting_in_loop_without_overlap_entire_table(
    self, partitions=10, parts_per_partition=1, block_size=100
):
    """Check that clickhouse support concurrent DELETE statement which is related to replicated table
    by creating replicated table and deleting all rows in the table by partitions on different nodes without overlap.
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
            f"SELECT count(*) FROM {table_name} WHERE NOT(x<33 OR (x>32 AND x<68) OR x>67)"
        ).output

    with Then("I perform concurrent deletes"):
        Step(
            name="delete from clickhouse1 by partitions",
            test=delete_in_loop,
            parallel=True,
        )(
            partitions=partitions,
            table_name=table_name,
            condition="WHERE x < 33",
            node=self.context.cluster.node("clickhouse1"),
        )

        Step(
            name="delete from clickhouse2 by partitions",
            test=delete_in_loop,
            parallel=True,
        )(
            partitions=partitions,
            table_name=table_name,
            condition="WHERE x > 32 AND x < 68",
            node=self.context.cluster.node("clickhouse2"),
        )

        Step(
            name="delete from clickhouse3 by partitions",
            test=delete_in_loop,
            parallel=True,
        )(
            partitions=partitions,
            table_name=table_name,
            condition="WHERE x > 67",
            node=self.context.cluster.node("clickhouse3"),
        )
    for attempt in retries(delay=1, timeout=30):
        with attempt:
            check_query_on_all_nodes(
                query=f"SELECT count(*) FROM {table_name}", output=expected_output
            )


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_EventualConsistency("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_RowsRemovedFromReplica("1.0"),
)
def replicated_table_deleting_in_loop_with_overlap_entire_table(
    self, partitions=10, parts_per_partition=1, block_size=100
):
    """Check that clickhouse support concurrent DELETE statement which is related to replicated table
    by creating replicated table and deleting all rows in the table by partitions on different nodes with overlap.
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
            f"SELECT count(*) FROM {table_name} WHERE NOT(x<50 OR (x>25 AND x<75) OR x>50)"
        ).output

    with Then("I perform concurrent deletes"):
        By(name="deleting from clickhouse1", test=delete_in_loop, parallel=True)(
            partitions=partitions,
            table_name=table_name,
            condition="WHERE x < 50",
            node=self.context.cluster.node("clickhouse1"),
        )

        By(name="deleting from clickhouse2", test=delete_in_loop, parallel=True)(
            partitions=partitions,
            table_name=table_name,
            condition="WHERE x > 25 AND x < 75",
            node=self.context.cluster.node("clickhouse2"),
        )

        By(name="deleting from clickhouse3", test=delete_in_loop, parallel=True)(
            partitions=partitions,
            table_name=table_name,
            condition="WHERE x > 50",
            node=self.context.cluster.node("clickhouse3"),
        )
    for attempt in retries(delay=1, timeout=30):
        with attempt:
            check_query_on_all_nodes(
                query=f"SELECT count(*) FROM {table_name}", output=expected_output
            )


@TestFeature
@Requirements()
@Name("replicated tables concurrent deletes")
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
