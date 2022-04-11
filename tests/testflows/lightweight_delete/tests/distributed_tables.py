from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
def distributed_table_replicated(self):
    """Check that ClickHouse supports delete statement for distributed tables
    when distributed table configuration contains 1 shard and 3 replicas.
    """
    table_name = getuid()

    with Given(
        "I create replicated table on cluster",
        description="""
            Using the following cluster configuration containing 1 shard with 3 replicas:
                shard0: 
                    replica0: clickhouse1
                    replica1: clickhouse2
                    replica2: clickhouse3
            """,
    ):
        for i, name in enumerate(self.context.cluster.nodes["clickhouse"]):
            node = self.context.cluster.node(name)
            self.context.node = node
            create_partitioned_table(
                node=node,
                table_name=table_name,
                engine=f"ReplicatedMergeTree('/clickhouse/tables/shard0/{table_name}', 'replica{i}')",
            )

    with When(f"I create distributed table"):
        self.context.node = node = self.context.cluster.node("clickhouse1")
        create_partitioned_table(
            node=node,
            table_name=table_name + "_distributed",
            cluster="ON CLUSTER replicated_cluster",
            partition="",
            order="",
            engine=f"Distributed('replicated_cluster', default, {table_name}, rand(), 'default')",
        )

    with When(f"I insert into table on clickhouse1 node"):
        name = "clickhouse1"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I insert into distributed table"):
            insert(
                table_name=table_name + "_distributed",
                partitions=10,
                parts_per_partition=1,
                block_size=100,
                settings=[("insert_distributed_sync", "1")],
            )

    with Then("I expect i can select from replicated table on any node"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully inserted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}_distributed")
                assert r.output == "1000", error()

    with Then("I delete odd rows from table on clickhouse1 node"):
        name = "clickhouse1"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete odd rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x % 2 == 0")

    with Then("I expect data is deleted from distributed table"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}_distributed")
                assert r.output == "500", error()


@TestScenario
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_MultipleShards("1.0"))
def distributed_table_sharded(self):
    """Check that ClickHouse supports delete statement for distributed tables
    when distributed table configuration contains 3 shard 1 replica in each.
    """
    table_name = getuid()

    with Given(
        "I create replicated table on cluster",
        description="""
            Using the following cluster configuration containing 1 shard with 3 replicas:
                shard0:
                    replica0: clickhouse1
                shard1:
                    replica0: clickhouse2
                shard1:
                    replica0: clickhouse3   
            """,
    ):
        for i, name in enumerate(self.context.cluster.nodes["clickhouse"]):
            node = self.context.cluster.node(name)
            self.context.node = node
            create_partitioned_table(
                node=node,
                table_name=table_name,
                engine=f"ReplicatedMergeTree('/clickhouse/tables/shard{i}/{table_name}', 'replica0')",
            )

    with When(f"I create distributed table"):
        self.context.node = node = self.context.cluster.node("clickhouse1")
        create_partitioned_table(
            node=node,
            table_name=table_name + "_distributed",
            cluster="ON CLUSTER sharded_cluster",
            partition="",
            order="",
            engine=f"Distributed('sharded_cluster', default, {table_name}, rand(), 'default')",
        )

    with When(f"I insert into table on clickhouse1 node"):
        name = "clickhouse1"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I insert into distributed table"):
            insert(
                table_name=table_name + "_distributed",
                partitions=10,
                parts_per_partition=1,
                block_size=100,
                settings=[("insert_distributed_sync", "1")],
            )

    with Then("I expect i can select from replicated table on any node"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully inserted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}_distributed")
                assert r.output == "1000", error()

    with Then("I delete odd rows from table on clickhouse1 node"):
        name = "clickhouse1"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I delete odd rows from table on {name} node"):
            delete(table_name=table_name, condition="WHERE x % 2 == 0")

    with Then("I expect data is deleted from distributed table"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}_distributed")
                assert int(r.output) >= 500, error()


@TestFeature
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_EventualConsistency("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_RowsRemovedFromReplica("1.0"),
)
@Name("distributed tables")
def feature(self):
    """Check that clickhouse supports delete statement with distributed tables."""
    self.context.table_engine = "MergeTree"

    for scenario in loads(current_module(), Scenario):
        scenario()
