from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_Performance("1.0"))
def performance_without_primary_key(self, node=None):
    """Check that clickhouse have similar performance between delete and select statements without primary key."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_partitioned_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name,
            partitions=100,
            parts_per_partition=1,
            block_size=100000,
        )

    start_time = time.time()

    with When(f"I mark the time that spended on select query"):
        r = node.query(f"SELECT count(*) FROM {table_name} WHERE x % 2 = 0")

    execution_time1 = time.time() - start_time

    start_time = time.time()

    with When(f"I delete all rows from the table"):
        delete(table_name=table_name, condition="WHERE x % 2 = 0")

    execution_time2 = time.time() - start_time

    with Then("I check performance"):
        assert (
            0.01 * execution_time1 < execution_time2 < 100 * execution_time1
        ), error()  # todo rewrite value


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Performance("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_Performance_LargeNumberOfPartitions("1.0"),
)
def performance_with_primary_key_many_partitions(self, node=None):
    """Check that clickhouse have similar performance between delete and select statements with primary key."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_partitioned_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name,
            partitions=100,
            parts_per_partition=1,
            block_size=100000,
        )

    start_time = time.time()

    with When(f"I mark the time that was spent on select query"):
        r = node.query(f"SELECT count(*) FROM {table_name} WHERE id % 2 = 0")

    execution_time1 = time.time() - start_time

    start_time = time.time()

    with When(f"I mark the time that was spent on delete query"):
        delete(table_name=table_name, condition="WHERE id % 2 = 0")

    execution_time2 = time.time() - start_time

    with Then("I check performance"):
        assert (
            0.01 * execution_time1 < execution_time2 < 100 * execution_time1
        ), error()  # todo rewrite value


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Performance("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_Performance_LargeNumberOfPartsInPartitions(
        "1.0"
    ),
)
def performance_with_primary_key_many_parts(self, node=None):
    """Check that clickhouse have similar performance between delete and select statements with primary key."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_partitioned_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name,
            partitions=100,
            parts_per_partition=100,
            block_size=1000,
        )

    start_time = time.time()

    with When(f"I mark the time that was spent on select query"):
        r = node.query(f"SELECT count(*) FROM {table_name} WHERE id % 2 = 0")

    execution_time1 = time.time() - start_time

    start_time = time.time()

    with When(f"I mark the time that was spent on delete query"):
        delete(table_name=table_name, condition="WHERE id % 2 = 0")

    execution_time2 = time.time() - start_time

    with Then("I check performance"):
        assert (
            0.01 * execution_time1 < execution_time2 < 100 * execution_time1
        ), error()  # todo rewrite value


@TestScenario
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_Performance_PostDelete("1.0"))
def performance_post_delete_select(self, node=None):
    """Check that clickhouse select statement performance is not degrade or degrade insignificantly on
    tables that contain rows deleted using the DELETE statement.
    """

    if node is None:
        node = self.context.node

    table_name_1 = f"table_{getuid()}_1"
    table_name_2 = f"table_{getuid()}_2"

    with Given("I have a table 1"):
        create_partitioned_table(table_name=table_name_1)

    with Given("I have a table 2"):
        create_partitioned_table(table_name=table_name_2)

    with When("I insert a lot of data into the first table"):
        insert(
            table_name=table_name_1,
            partitions=100,
            parts_per_partition=1,
            block_size=100000,
        )

    with When("I insert a lot of data into the second table"):
        insert(
            table_name=table_name_2,
            partitions=10,
            parts_per_partition=1,
            block_size=100000,
        )

    with Then("I delete a lot of data from the first table"):
        delete(table_name=table_name_1, condition="WHERE id > 10")

    start_time = time.time()

    with When(f"I mark the time that was spent on delete query"):
        r1 = node.query(f"SELECT count(*) from {table_name_1}")

    execution_time1 = time.time() - start_time
    metric("execution_time1", execution_time1, "s")

    start_time = time.time()

    with When(f"I mark the time that was spent on delete query"):
        r2 = node.query(f"SELECT count(*) from {table_name_2}")

    execution_time2 = time.time() - start_time
    metric("execution_time2", execution_time2, "s")

    with Then("I compare time spent for select statement with and without delete"):
        assert (
            1.1 * execution_time1 > execution_time2
        ), error()  # todo rewrite values after implementation


@TestFeature
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_Performance("1.0"))
@Name("performance")
def feature(self, node="clickhouse1"):
    """Check that clickhouse lightweight delete statement has good performance."""
    self.context.node = self.context.cluster.node(node)
    self.context.table_engine = "MergeTree"
    for scenario in loads(current_module(), Scenario):
        scenario()
