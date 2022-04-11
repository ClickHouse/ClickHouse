from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_MultipleDeletes("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_MultipleDeletes_Limitations("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_DeletesPerSecond("1.0"),
)
def multiple_delete(self, node=None):
    """Check that clickhouse supports using multiple DELETE statements on the same table."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name, partitions=10, parts_per_partition=1, block_size=100
        )

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x < 10)"
        ).output

    with When(f"I perform multiple delete"):
        condition = [f"WHERE x = {i}" for i in range(10)]
        delete(table_name=table_name, condition=condition, multiple=True)

    with Then("I check that rows are deleted"):
        for attempt in retries(delay=1, timeout=30):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestFeature
@Name("multiple delete limitations")
def feature(self, node="clickhouse1"):
    """Check that clickhouse supports using multiple DELETE statements on the same table."""
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
