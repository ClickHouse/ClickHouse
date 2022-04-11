from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
def replicated_table_deleting_without_overlap(self):
    """Check that clickhouse support DELETE statement which is related to replicated table."""
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT((x % 2 == 0 AND id < 5)"
            f" OR (x % 2 == 1 AND id < 5))"
        ).output

    with Then("I delete odd rows from table on clickhouse1 node"):
        name = "clickhouse1"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete odd rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x % 2 == 0 AND id < 5")

    with Then("I delete even rows from table on clickhouse2 node"):
        name = "clickhouse2"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete even rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x % 2 == 1 AND id < 5")

    with Then("I expect data is deleted in tables on any node"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == expected_output, error()


@TestScenario
def replicated_table_deleting_with_overlap(self):
    """Check that clickhouse support DELETE statement which is related to replicated table."""
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT((x % 2 == 0 AND id < 5)"
            f" OR (x % 2 == 0 AND id < 5))"
        ).output

    with Then("I delete odd rows from table on clickhouse1 node"):
        name = "clickhouse1"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete odd rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x % 2 == 0 AND id < 5")

    with Then("I delete odd rows from table on clickhouse2 node"):
        name = "clickhouse2"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete even rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x % 2 == 0 AND id < 5")

    with Then("I expect data is deleted in tables on any node"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == expected_output, error()


@TestScenario
def replicated_table_deleting_with_without_overlap(self):
    """Check that clickhouse support DELETE statement which is related to replicated table."""
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT((x % 2 == 0 AND id < 50)"
            f" OR (x % 2 == 0 AND id < 50) OR (x % 2 == 0 AND id < 75 AND id > 24))"
        ).output

    with Then("I delete odd rows from table on clickhouse1 node"):
        name = "clickhouse1"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete odd rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x % 2 == 0 AND id < 50")

    with Then("I delete odd rows from table on clickhouse2 node"):
        name = "clickhouse2"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete even rows from table on {name} node"):
            delete(
                table_name=table_name,
                condition="WHERE x % 2 == 0 AND id < 75 AND id > 24",
            )

    with Then("I expect data is deleted in tables on any node"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == expected_output, error()


@TestScenario
def replicated_table_deleting_without_overlap_entire_table(self):
    """Check that clickhouse support DELETE statement which is related to replicated table."""
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x<33 OR (x>32 AND x<68) OR x>67)"
        ).output

    with Then("I delete odd rows from table on clickhouse1 node"):
        name = "clickhouse1"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete odd rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x < 33")

    with Then("I delete odd rows from table on clickhouse2 node"):
        name = "clickhouse2"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete even rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x > 32 AND x < 68")

    with Then("I delete odd rows from table on clickhouse3 node"):
        name = "clickhouse3"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete even rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x > 67")

    with Then("I expect data is deleted in tables on any node"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == expected_output, error()


@TestScenario
def replicated_table_deleting_with_overlap_entire_table(self):
    """Check that clickhouse support DELETE statement which is related to replicated table."""
    table_name = getuid()

    create_table(table_name=table_name)
    insert_replicated(
        table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
    )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x<50 OR (x>25 AND x<75) OR x>50)"
        ).output

    with Then("I delete odd rows from table on clickhouse1 node"):
        name = "clickhouse1"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete odd rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x < 50")

    with Then("I delete odd rows from table on clickhouse2 node"):
        name = "clickhouse2"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete even rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x > 25 AND x < 75")

    with Then("I delete odd rows from table on clickhouse3 node"):
        name = "clickhouse3"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete even rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x > 50")

    with Then("I expect data is deleted in tables on any node"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == expected_output, error()


@TestFeature
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_EventualConsistency("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_RowsRemovedFromReplica("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_MultipleReplicas("1.0"),
)
@Name("replicated tables")
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
