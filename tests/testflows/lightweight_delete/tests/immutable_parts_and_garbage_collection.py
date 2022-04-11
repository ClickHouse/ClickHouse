from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_ImmutablePartsAndGarbageCollection("1.0")
)
def immutable_parts_and_garbage_collection(self, hash_utility="sha1sum", node=None):
    """Check that parts affected by the DELETE statement are stay immutable and
    the deleted rows are garbage collected in a scheduled merge.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name, partitions=100, parts_per_partition=1, block_size=100
        )

    with When("I stop merges"):
        node.query("SYSTEM STOP MERGES")

    with When("I compute path"):
        path = node.query(
            f"SELECT path FROM system.parts WHERE table = '{table_name}'"
            + " ORDER BY partition LIMIT 1"
        ).output

    with Then("I compute hash of part file"):
        hash1 = node.command(f"cat {path}data.bin | {hash_utility}").output

    with And("I delete while merges are stopped"):
        delete(
            table_name=table_name, condition="WHERE id = 0 AND (x % 2 = 0)", settings=[]
        )

    with Then("I check hash is not changed"):
        hash2 = node.command(f"cat {path}data.bin | {hash_utility}").output
        assert hash1 == hash2


@TestFeature
@Name("immutable parts and garbage collection")
def feature(self, node="clickhouse1"):
    """Check that parts affected by the DELETE statement are stay immutable and
    the deleted rows are garbage collected in a scheduled merge.
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
