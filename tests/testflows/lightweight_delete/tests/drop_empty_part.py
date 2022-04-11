from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_DropEmptyPart("1.0"))
def drop_empty_part(self, node=None):
    """Check that clickhouse schedules dropping the part if all of the rows are deleted from this part."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name, partitions=100, parts_per_partition=1, block_size=100
        )

    with When("I check partition 0 exists in the system.parts table"):
        r = node.query(
            f"SELECT partition FROM system.parts WHERE table = '{table_name}' and active = 1 \
        ORDER BY partition LIMIT 1"
        )
        assert r.output == "0", error()

    with Then("I delete part 0 and force merges"):
        delete(table_name=table_name, condition="WHERE id = 0")
        optimize_table(table_name=table_name)

    with When("I check partition is deleted"):
        for attempt in retries(timeout=100, delay=1):
            with attempt:
                r = node.query(
                    f"SELECT partition FROM system.parts WHERE table = '{table_name}' and active = 1 \
                ORDER BY partition LIMIT 1"
                )
                assert r.output == "1", error()


@TestFeature
@Name("drop empty part")
def feature(self, node="clickhouse1"):
    """Check that clickhouse schedules dropping the part if all of the rows are deleted from this part."""
    self.context.node = self.context.cluster.node(node)

    self.context.table_engine = "MergeTree"

    for scenario in loads(current_module(), Scenario):
        scenario()
