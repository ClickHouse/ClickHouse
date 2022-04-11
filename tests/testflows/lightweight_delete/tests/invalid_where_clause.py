from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_InvalidSyntax_EmptyWhere("1.0"),
)
def empty_where_clause(self, node=None):
    """Check that clickhouse returns an error when using DELETE statement with empty WHERE clause."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert some data into the table"):
        insert(
            table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
        )

    with When(f"I try to delete from {table_name} with empty WHERE clause"):
        r = delete(table_name=table_name, condition="WHERE", no_checks=True)

    with Then("I check that exitcode is not 0"):
        assert r.exitcode != 0


@TestScenario
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_InvalidSyntax_NoWhere("1.0"))
def no_where(self, node=None):
    """Check that clickhouse returns an error when using DELETE statement with no WHERE clause."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert some data into the table"):
        insert(
            table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
        )

    with When(f"I try to delete from {table_name} without WHERE clause"):
        r = delete(table_name=table_name, condition="", no_checks=True)

    with Then("I check that exitcode is not 0"):
        assert r.exitcode != 0


@TestFeature
@Name("invalid where clause")
def feature(self, node="clickhouse1"):
    """Check that clickhouse returns an error when using DELETE statement with invalid WHERE clause."""
    self.context.node = self.context.cluster.node(node)

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
