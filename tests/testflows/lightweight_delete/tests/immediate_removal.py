from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_ImmediateRemovalForSelects("1.0"))
def delete_most_of_the_table(self, node=None):
    """Check that clickhouse immediately remove all rows for subsequent SELECTs
    after DELETE statement is executed.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name,
            partitions=100,
            parts_per_partition=1,
            block_size=100000,
        )

    with When("I compute expected output"):
        output = node.query(f"SELECT count(*) FROM {table_name} WHERE NOT(id>0)").output

    start_time = time.time()

    with When(f"I delete all rows from the table"):
        delete(table_name=table_name, condition="WHERE id > 0")

    execution_time = time.time() - start_time
    metric("execution_time", execution_time, "s")

    with Then("I check that rows are deleted immediately"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == output, error()
        assert execution_time < 100, error()  # todo rewrite value


@TestFeature
@Name("immediate removal")
def feature(self, node="clickhouse1"):
    """Check that clickhouse immediately remove all rows for subsequent SELECTs
    after DELETE statement is executed and the subsequent SELECT statements
    not apply the original WHERE conditions specified in the DELETE.
    """
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
