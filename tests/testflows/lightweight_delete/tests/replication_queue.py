from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *
import random
import datetime


@TestScenario
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_ReplicationQueue("1.0"))
def replication_queue(self, node=None):
    """Check that clickhouse writes delete statement in replication queue correctly
    by creating replicated table, deleting from it and checking that DELETE statement
    appeared in system.replication_queue table.
    """

    table_name = getuid()

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert data in the table"):
        insert_replicated(
            table_name=table_name, partitions=100, parts_per_partition=1, block_size=100
        )

    name = "clickhouse1"
    self.context.node = node = self.context.cluster.node(name)

    with When("I compute expected output"):
        expected_output = node.query(
            f"SELECT count(*) FROM {table_name}" f" WHERE NOT(x % 2 == 0)"
        ).output

    with Then("I delete odd rows from table on clickhouse1 node"):
        name = "clickhouse1"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete odd rows from table on {name} node"):
            delete(
                table_name=table_name,
                condition="WHERE x % 2 == 0",
                settings=[("mutations_sync", 0)],
            )

    with Then("I check replication queue"):
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                r1 = node.query(
                    f"SELECT create_time FROM system.replication_queue WHERE table='{table_name}' LIMIT 1"
                )
                assert r1.output != ""
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                r2 = node.query(
                    f"SELECT event_time FROM system.query_log WHERE query like '%{table_name}%DELETE%' LIMIT 1"
                )
                assert r2.output != ""

        replication_time = datetime.datetime.strptime(r1.output, "%Y-%m-%d %H:%M:%S")
        delete_time = datetime.datetime.strptime(r2.output, "%Y-%m-%d %H:%M:%S")

        assert abs(replication_time.toordinal() - delete_time.toordinal()) < 2, error()

    with Then("I check data is deleted"):
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                check_query_on_all_nodes(
                    query=f"SELECT count(*) FROM {table_name}", output=expected_output
                )


@TestFeature
@Name("replication queue")
def feature(self, node="clickhouse1"):
    """Check that clickhouse write delete statement in replication queue correctly."""

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
