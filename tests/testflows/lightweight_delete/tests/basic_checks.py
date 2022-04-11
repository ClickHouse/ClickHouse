from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestOutline
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_DeleteZeroRows("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_ServerRestart("1.0"),
)
def delete_zero_rows(self, table_name, node=None):
    """Check that DELETE can remove zero rows."""
    if node is None:
        node = self.context.node

    with When("I record the table data"):
        pre_delete = node.query(
            f"SELECT count(*) FROM {table_name} FORMAT JSONEachRow"
        ).output

    with When(f"I delete zero rows from {table_name}"):
        delete(table_name=table_name, condition="WHERE id < -1")

    with Then(f"I check {table_name} is empty"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} FORMAT JSONEachRow"
        ).output
        assert output == pre_delete, error()

    with And("I restart clickhouse"):
        node.restart_clickhouse()

    with Then("I check the data is not changed after restart"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} FORMAT JSONEachRow"
        ).output
        assert output == pre_delete, error()


@TestOutline
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_DeleteAllRows("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_ServerRestart("1.0"),
)
def delete_all_rows(self, table_name, node=None):
    """Check that DELETE can remove all rows."""
    if node is None:
        node = self.context.node

    with When(f"I delete all rows from {table_name}"):
        delete(table_name=table_name, condition="WHERE id > -1")

    with Then(f"I check {table_name} is empty"):
        output = node.query(
            f"SELECT * FROM {table_name} ORDER BY id, x FORMAT JSONEachRow"
        ).output
        assert output == "", error()

    with Then("I check row count"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        assert output == "0", error()

    with And("I restart clickhouse"):
        node.restart_clickhouse()

    with Then("I check the data is not changed after restart"):
        output = node.query(
            f"SELECT * FROM {table_name} ORDER BY id, x FORMAT JSONEachRow"
        ).output
        assert output == "", error()

    with Then("I check row count after restart"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        assert output == "0", error()


@TestOutline
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_DeleteSmallSubsetOfRows("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_ServerRestart("1.0"),
)
def delete_small_subset(self, table_name, node=None):
    """Check that DELETE can remove a small subset of rows."""
    if node is None:
        node = self.context.node

    count_all = node.query(f"SELECT count(*) FROM {table_name}").output
    count_del = node.query(f"SELECT count(*) FROM {table_name} WHERE x < 10").output
    expected_count = str(int(count_all) - int(count_del))

    with When(f"I delete all rows from {table_name}"):
        delete(table_name=table_name, condition="WHERE x < 10")

    with Then(f"I check {table_name} is empty"):
        output = node.query(
            f"SELECT * FROM {table_name} WHERE x < 10 ORDER BY id, x FORMAT JSONEachRow"
        ).output
        assert output == "", error()

    with Then(f"I check row count"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        assert output == expected_count, error()

    with And("I restart clickhouse"):
        node.restart_clickhouse()

    with Then("I check the data is not changed after restart"):
        output = node.query(
            f"SELECT * FROM {table_name} WHERE x < 10 ORDER BY id, x FORMAT JSONEachRow"
        ).output
        assert output == "", error()

    with Then(f"I check row count after restart"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        assert output == expected_count, error()


@TestOutline
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_DeleteLargeSubsetOfRows("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_ServerRestart("1.0"),
)
def delete_large_subset(self, table_name, node=None):
    """Check that DELETE can remove a large subset of rows."""
    if node is None:
        node = self.context.node

    count_all = node.query(f"SELECT count(*) FROM {table_name}").output
    count_del = node.query(f"SELECT count(*) FROM {table_name} WHERE x > 10").output
    expected_count = str(int(count_all) - int(count_del))

    with When(f"I delete all rows from {table_name}"):
        delete(table_name=table_name, condition="WHERE x > 10")

    with Then(f"I check {table_name} is empty"):
        output = node.query(
            f"SELECT * FROM {table_name} WHERE x > 10 ORDER BY id, x FORMAT JSONEachRow"
        ).output
        assert output == "", error()

    with Then(f"I check row count"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        assert output == expected_count, error()

    with And("I restart clickhouse"):
        node.restart_clickhouse()

    with Then("I check the data is not changed after restart"):
        output = node.query(
            f"SELECT * FROM {table_name} WHERE x > 10 ORDER BY id, x FORMAT JSONEachRow"
        ).output
        assert output == "", error()

    with Then(f"I check row count after restart"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        assert output == expected_count, error()


@TestOutline
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_AllRowsFromHalfOfTheParts("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_ServerRestart("1.0"),
)
def delete_all_rows_from_half_of_parts(self, table_name, node=None):
    """Check that DELETE can remove all rows from half of the parts."""
    if node is None:
        node = self.context.node

    count_all = node.query(f"SELECT count(*) FROM {table_name}").output
    count_del = node.query(f"SELECT count(*) FROM {table_name} WHERE id < 5").output
    expected_count = str(int(count_all) - int(count_del))

    with When(f"I delete all rows from {table_name}"):
        delete(table_name=table_name, condition="WHERE id < 5")

    with Then(f"I check {table_name} is empty"):
        output = node.query(
            f"SELECT * FROM {table_name} WHERE id < 5 ORDER BY id, x FORMAT JSONEachRow"
        ).output
        assert output == "", error()

    with Then(f"I check row count"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        assert output == expected_count, error()

    with And("I restart clickhouse"):
        node.restart_clickhouse()

    with Then("I check the data is not changed after restart"):
        output = node.query(
            f"SELECT * FROM {table_name} WHERE id < 5 ORDER BY id FORMAT JSONEachRow"
        ).output
        assert output == "", error()

    with Then(f"I check row count after restart"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        assert output == expected_count, error()


@TestOutline
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_DeleteOneRow("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_ServerRestart("1.0"),
)
def delete_one_row(self, table_name, node=None):
    """Check that DELETE can remove one row."""
    if node is None:
        node = self.context.node

    count_all = node.query(f"SELECT count(*) FROM {table_name}").output

    with And("I have one distinct row"):
        if self.context.table_engine in (
            "MergeTree",
            "ReplacingMergeTree",
            "SummingMergeTree",
            "AggregatingMergeTree",
        ):
            node.query(f"INSERT INTO {table_name} VALUES (-1,-1)")

        if self.context.table_engine in (
            "CollapsingMergeTree",
            "VersionedCollapsingMergeTree",
        ):
            node.query(f"INSERT INTO {table_name} VALUES (-1,-1,1)")

        if self.context.table_engine == "GraphiteMergeTree":
            node.query(
                f"INSERT INTO {table_name} VALUES (-1,-1, '1', toDateTime(10), 10, 10)"
            )

    with When(f"I delete the row"):
        delete(table_name=table_name, condition="WHERE id < 0")

    with Then("I check the table is the same as before the insert"):
        output = node.query(
            f"SELECT * FROM {table_name} WHERE id < 0 ORDER BY id FORMAT JSONEachRow"
        ).output
        assert output == "", error()

    with Then(f"I check row count"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        assert output == count_all, error()

    with And("I restart clickhouse"):
        node.restart_clickhouse()

    with Then("I check the data is not changed after restart"):
        output = node.query(
            f"SELECT * FROM {table_name} WHERE id < 0 ORDER BY id FORMAT JSONEachRow"
        ).output
        assert output == "", error()

    with Then(f"I check row count after restart"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        assert output == count_all, error()


@TestFeature
@Name("one partition one part")
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_OnePartitionWithPart("1.0"))
def one_partition_one_part(self, node=None):
    """Check DELETE with a table that has one partition and one part."""
    if node is None:
        node = self.context.node

    for outline in loads(current_module(), Outline):
        with Scenario(test=outline):
            table_name = f"table_{getuid()}"

            with Given("I have a table"):
                create_table(table_name=table_name)

            with When("I insert some data into the table"):
                insert(
                    table_name=table_name,
                    partitions=1,
                    parts_per_partition=1,
                    block_size=1000,
                )

            outline(table_name=table_name)


@TestFeature
@Name("one partition many parts")
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_PartitionWithManyParts("1.0"))
def one_partition_many_parts(self, node=None):
    """Check DELETE with a table that has one partition and many parts."""
    if node is None:
        node = self.context.node

    for outline in loads(current_module(), Outline):
        with Scenario(test=outline):
            table_name = f"table_{getuid()}"

            with Given("I have a table"):
                create_table(table_name=table_name)

            with When("I insert some data into the table"):
                insert(
                    table_name=table_name,
                    partitions=1,
                    parts_per_partition=100,
                    block_size=1000,
                )

            outline(table_name=table_name)


@TestFeature
@Name("one partition mixed parts")
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_VeryLargePart("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_VerySmallPart("1.0"),
)
def one_partition_mixed_parts(self, node=None):
    """Check DELETE with a table that has one partition, one large part, and many small parts."""
    if node is None:
        node = self.context.node

    for outline in loads(current_module(), Outline):
        with Scenario(test=outline):
            table_name = f"table_{getuid()}"

            with Given("I have a table"):
                create_table(table_name=table_name)

            with When("I insert one large part"):
                insert(
                    table_name=table_name,
                    partitions=1,
                    parts_per_partition=1,
                    block_size=1000,
                )

            with And("I insert many small parts"):
                insert(
                    table_name=table_name,
                    partitions=1,
                    parts_per_partition=100,
                    block_size=10,
                )

            outline(table_name=table_name)


@TestFeature
@Name("many partition one part")
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_MultiplePartitionsAndOnePart("1.0")
)
def many_partition_one_part(self, node=None):
    """Check DELETE with a table that has many partition and one part."""
    if node is None:
        node = self.context.node

    for outline in loads(current_module(), Outline):
        with Scenario(test=outline):
            table_name = f"table_{getuid()}"

            with Given("I have a table"):
                create_table(table_name=table_name)

            with When("I insert some data into the table"):
                insert(
                    table_name=table_name,
                    partitions=10,
                    parts_per_partition=1,
                    block_size=1000,
                )

            outline(table_name=table_name)


@TestFeature
@Name("many partition many parts")
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_MultiplePartsAndPartitions("1.0"))
def many_partition_many_parts(self, node=None):
    """Check DELETE with a table that has many partition and many parts."""
    if node is None:
        node = self.context.node

    for outline in loads(current_module(), Outline):
        with Scenario(test=outline):
            table_name = f"table_{getuid()}"

            with Given("I have a table"):
                create_table(table_name=table_name)

            with When("I insert some data into the table"):
                insert(
                    table_name=table_name,
                    partitions=100,
                    parts_per_partition=100,
                    block_size=100,
                )

            outline(table_name=table_name)


@TestFeature
@Name("many partition mixed parts")
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_VeryLargePart("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_VerySmallPart("1.0"),
)
def many_partition_mixed_parts(self, node=None):
    """Check DELETE with a table that has many partition, each with one large part and many small parts."""
    if node is None:
        node = self.context.node

    for outline in loads(current_module(), Outline):
        with Scenario(test=outline):
            table_name = f"table_{getuid()}"

            with Given("I have a table"):
                create_table(table_name=table_name)

            with When("I insert one large part"):
                insert(
                    table_name=table_name,
                    partitions=1,
                    parts_per_partition=1,
                    block_size=10000,
                )

            with And("I insert many small parts"):
                insert(
                    table_name=table_name,
                    partitions=10,
                    parts_per_partition=100,
                    block_size=10,
                )

            outline(table_name=table_name)


@TestFeature
@Name("one million datapoints")
def one_million_datapoints(self, node=None):
    """Check DELETE with a table that has one million entries."""
    if node is None:
        node = self.context.node

    for outline in loads(current_module(), Outline):
        with Scenario(test=outline):
            table_name = f"table_{getuid()}"

            with Given("I have a table"):
                create_table(table_name=table_name)

            with When("I insert one million entries"):
                insert(
                    table_name=table_name,
                    partitions=100,
                    parts_per_partition=10,
                    block_size=1000,
                )

            outline(table_name=table_name)


@TestFeature
@Requirements()
@Name("basic checks")
def feature(self, node="clickhouse1"):
    """Run basic checks for the DELETE statement in ClickHouse."""
    self.context.node = self.context.cluster.node(node)

    self.context.table_engine = "MergeTree"

    Feature(run=one_million_datapoints)
    Feature(run=many_partition_mixed_parts)
    Feature(run=many_partition_many_parts)
    Feature(run=many_partition_one_part)
    Feature(run=one_partition_mixed_parts)
    Feature(run=one_partition_many_parts)
    Feature(run=one_partition_one_part)
