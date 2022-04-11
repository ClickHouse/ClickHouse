from copy import deepcopy
import random

from helpers.common import *
from xml.sax.saxutils import escape as xml_escape


@TestStep(Given)
def create_directory(self, path, node=None):
    """Creating directory on node"""
    if node is None:
        node = self.context.node
    try:
        node.command(f"mkdir '{path}'", exitcode=0)
        yield path
    finally:
        with Finally("delete directory", description=f"{path}"):
            node.command(f"rm -rf '{path}'", exitcode=0)


@TestStep(Given)
def create_directories_multi_disk_volume(self, number_of_disks):
    with Given("I create local disk folders on the server"):
        for i in range(number_of_disks):
            create_directory(path=f"disk_local{i}/")


@TestStep(Given)
def add_config_multi_disk_volume(self, number_of_disks):
    entries = {
        "storage_configuration": {
            "disks": [],
            "policies": {"local": {"volumes": {"volume1": []}}},
        }
    }

    with Given("I set up parameters"):
        entries_in_this_test = deepcopy(entries)
        for i in range(number_of_disks):

            entries_in_this_test["storage_configuration"]["disks"].append(
                {f"local{i}": {"path": f"/disk_local{i}/"}}
            )

            entries_in_this_test["storage_configuration"]["policies"]["local"][
                "volumes"
            ]["volume1"].append({"disk": f"local{i}"})

    with And("I add storage configuration that uses disk"):
        add_disk_configuration(entries=entries_in_this_test, restart=True)


@TestStep(Given)
def add_disk_configuration(
    self, entries, xml_symbols=True, modify=False, restart=True, format=None, user=None
):
    """Create disk configuration file and add it to the server."""
    with By("converting config file content to xml"):
        config = create_xml_config_content(entries, "storage_conf.xml")
        if format is not None:
            for key, value in format.items():
                if xml_symbols:
                    config.content = config.content.replace(key, xml_escape(value))
                else:
                    config.content = config.content.replace(key, value)
    with And("adding xml config file to the server"):
        return add_config(config, restart=restart, modify=modify, user=user)


@TestStep(Given)
def add_ontime_table(self, node=None):
    """Unpack ontime table into clickhouse dataset."""
    if node is None:
        node = self.context.node

    try:
        with Given(f"I have ontime table"):
            node.command("cp -R /ontime/data/* /var/lib/clickhouse/data")
            node.command("cp -R /ontime/metadata/* /var/lib/clickhouse/metadata")

        yield

    finally:
        with Finally("I remove the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS datasets.ontime SYNC")


@TestStep(Given)
def create_partitioned_table(
    self,
    table_name,
    extra_table_col="",
    cluster="",
    engine="MergeTree",
    partition="PARTITION BY id",
    order="ORDER BY id",
    settings="",
    node=None,
    options="",
):
    """Create a partitioned table."""
    if node is None:
        node = self.context.node

    try:
        with Given(f"I have a table {table_name}"):
            node.query(
                f"CREATE TABLE {table_name} {cluster} (id Int64, x Int64{extra_table_col})"
                f" Engine = {engine} {partition} {order} {options} {settings}"
            )

        yield

    finally:
        with Finally("I remove the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")


@TestStep(When)
def create_replacing_merge_tree_table(
    self, table_name, node=None, settings="", options=""
):
    """Create replacing merge tree table."""

    create_partitioned_table(
        table_name=table_name,
        engine="ReplacingMergeTree()",
        node=node,
        settings=settings,
        options=options,
    )


@TestStep(When)
def create_summing_merge_tree_table(
    self, table_name, node=None, settings="", options=""
):
    """Create summing merge tree table."""

    create_partitioned_table(
        table_name=table_name,
        engine="SummingMergeTree(x)",
        node=node,
        settings=settings,
        options=options,
    )


@TestStep(When)
def create_aggregating_merge_tree_table(
    self, table_name, node=None, settings="", options=""
):
    """Create aggregating merge tree table."""

    create_partitioned_table(
        table_name=table_name,
        engine="AggregatingMergeTree()",
        node=node,
        settings=settings,
        options=options,
    )


@TestStep(When)
def create_collapsing_merge_tree_table(
    self, table_name, node=None, settings="", options=""
):
    """Create collapsing merge tree table."""

    create_partitioned_table(
        table_name=table_name,
        extra_table_col=" ,Sign Int8",
        engine="CollapsingMergeTree(Sign)",
        node=node,
        settings=settings,
        options=options,
    )


@TestStep(When)
def create_versioned_collapsing_merge_tree_table(
    self, table_name, node=None, settings="", options=""
):
    """Create versioned collapsing merge tree table."""

    create_partitioned_table(
        table_name=table_name,
        extra_table_col=" ,Sign Int8",
        engine="VersionedCollapsingMergeTree(Sign, id)",
        node=node,
        settings=settings,
        options=options,
    )


@TestStep(When)
def create_graphite_merge_tree_table(
    self, table_name, node=None, settings="", options=""
):
    """Create graphite merge tree table."""

    create_partitioned_table(
        table_name=table_name,
        extra_table_col=" ,Path String, Time DateTime, Value Int64, Timestamp Int64",
        engine="GraphiteMergeTree('graphite_rollup_example')",
        node=node,
        settings=settings,
        options=options,
    )


@TestStep(When)
def create_replicated_table(self, table_name):
    """Create replicated table with 3 replicas."""

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


@TestStep(When)
def create_replicated_replacing_merge_tree_table(self, table_name):
    """Create replicated table with 3 replicas."""

    with Given(
        "I create replicated replacing merge tree table on cluster",
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
                engine=f"ReplicatedReplacingMergeTree('/clickhouse/tables/shard0/{table_name}', 'replica{i}')",
            )


@TestStep(When)
def create_replicated_summing_merge_tree_table(self, table_name):
    """Create replicated table with 3 replicas."""

    with Given(
        "I create replicated summing merge tree table on cluster",
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
                engine=f"ReplicatedSummingMergeTree('/clickhouse/tables/shard0/{table_name}', 'replica{i}', x)",
            )


@TestStep(When)
def create_replicated_aggregating_merge_tree_table(self, table_name):
    """Create replicated table with 3 replicas."""

    with Given(
        "I create replicated aggregating merge tree table on cluster",
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
                engine=f"ReplicatedAggregatingMergeTree('/clickhouse/tables/shard0/{table_name}', 'replica{i}')",
            )


@TestStep(When)
def create_replicated_collapsing_merge_tree_table(self, table_name):
    """Create replicated table with 3 replicas."""

    with Given(
        "I create replicated collapsing merge tree table on cluster",
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
                extra_table_col=" ,Sign Int8",
                engine=f"ReplicatedCollapsingMergeTree('/clickhouse/tables/shard0/{table_name}', 'replica{i}', Sign)",
            )


@TestStep(When)
def create_replicated_versioned_collapsing_merge_tree_table(self, table_name):
    """Create replicated table with 3 replicas."""

    with Given(
        "I create replicated versioned collapsing merge tree table on cluster",
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
                extra_table_col=" ,Sign Int8",
                engine=f"ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/shard0/{table_name}', 'replica{i}', Sign, id)",
            )


@TestStep(When)
def create_replicated_graphite_merge_tree_table(self, table_name):
    """Create replicated table with 3 replicas."""

    with Given(
        "I create replicated graphite merge tree table on cluster",
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
                table_name=table_name,
                extra_table_col=" ,Path String, Time DateTime, Value Int64, Timestamp Int64",
                engine=f"ReplicatedGraphiteMergeTree('/clickhouse/tables/shard0/{table_name}', 'replica{i}', 'graphite_rollup_example')",
                node=node,
            )


@TestStep(When)
def insert(
    self,
    table_name,
    values=None,
    partitions=1,
    parts_per_partition=1,
    block_size=1000,
    no_checks=False,
    settings=[],
    node=None,
    table_engine=None,
):
    """Insert data having specified number of partitions and parts."""
    if node is None:
        node = self.context.node

    if table_engine is None:
        table_engine = self.context.table_engine

    if values is None:
        if table_engine in (
            "MergeTree",
            "ReplacingMergeTree",
            "SummingMergeTree",
            "AggregatingMergeTree",
        ):
            values = ",".join(
                f"({x},{y})"
                for x in range(partitions)
                for y in range(block_size * parts_per_partition)
            )

        if table_engine in ("CollapsingMergeTree", "VersionedCollapsingMergeTree"):
            values = ",".join(
                f"({x},{y},1)"
                for x in range(partitions)
                for y in range(block_size * parts_per_partition)
            )

        if table_engine == "GraphiteMergeTree":
            values = ",".join(
                f"({x},{y}, '1', toDateTime(10), 10, 10)"
                for x in range(partitions)
                for y in range(block_size * parts_per_partition)
            )

    return node.query(
        f"INSERT INTO {table_name} VALUES {values}",
        settings=[("max_block_size", block_size)] + settings,
        no_checks=no_checks,
    )


@TestStep(When)
def insert_replicated(
    self,
    table_name,
    values=None,
    partitions=1,
    parts_per_partition=1,
    block_size=1000,
    settings=[],
    table_engine=None,
):
    """Insert data into replicated table."""

    if table_engine is None:
        table_engine = self.context.table_engine

    with When(f"I insert into table on clickhouse1 node"):
        name = "clickhouse1"
        self.context.node = node = self.context.cluster.node(name)
        with When(f"I insert into table on {name} node"):
            insert(
                table_name=table_name,
                values=values,
                partitions=partitions,
                parts_per_partition=parts_per_partition,
                block_size=block_size,
                table_engine=table_engine[10:],
                settings=[("insert_distributed_sync", "1")] + settings,
            )


@TestStep(When)
def delete(
    self,
    table_name,
    condition,
    use_alter_delete=False,
    optimize=True,
    no_checks=False,
    check=False,
    delay=0,
    settings=[("mutations_sync", 2)],
    node=None,
    multiple=False,
):
    """Delete rows from table that match the condition.'""?????????????????::"""
    if node is None:
        node = self.context.node

    if use_alter_delete or self.context.use_alter_delete:
        if multiple:
            command = ";".join(
                [f"ALTER TABLE {table_name} DELETE {i}" for i in condition]
            )
        else:
            command = f"ALTER TABLE {table_name} DELETE {condition}"
        r = node.query(command, no_checks=no_checks, settings=settings)
        if check:
            with Then("I check rows are deleted"):
                check_result = node.query(
                    f"SELECT count() FROM {table_name} {condition}"
                )
                assert check_result.output == "0", error()
        if delay:
            time.sleep(delay)
        return r
    # FIXME: change to DELETE when there is actual implementation
    if multiple:
        command = ";".join(
            [f"DELETE FROM TABLE {table_name} WHERE {i}" for i in condition]
        )
    else:
        command = f"DELETE FROM TABLE {table_name} WHERE {condition}"
    r = node.query(command, no_checks=no_checks, settings=settings)
    if check:
        with Then("I check rows are deleted"):
            check_result = node.query(f"SELECT count() FROM {table_name} {condition}")
            assert check_result.output == "0", error()
    if delay:
        time.sleep(delay)
    return r


@TestStep(When)
def optimize_table(self, table_name, final=True, node=None):
    """Force merging of some (final=False) or all parts (final=True)
    by calling OPTIMIZE TABLE.
    """
    if node is None:
        node = self.context.node

    query = f"OPTIMIZE TABLE {table_name}"
    if final:
        query += " FINAL"

    return node.query(query)


@TestStep(Given)
def stop_merges(self, node=None):
    """Stop merges."""
    if node is None:
        node = self.context.node
    try:
        node.query("SYSTEM STOP MERGES")
        yield
    finally:
        with Finally("I restart merges"):
            node.query("SYSTEM START MERGES")


@TestStep
def delete_odd(
    self, num_partitions, table_name, settings=[("mutations_sync", 2)], node=None
):
    """Delete all odd `x` rows in all partitions."""
    for i in range(num_partitions):
        delete(
            table_name=table_name,
            condition=f"WHERE (id = {i}) AND (x % 2 = 0)",
            delay=random.random() / 10,
            settings=settings,
            node=node,
        )


@TestStep
def alter_detach_partition(
    self, table_name, partition_expr="7", node=None, settings=[("mutations_sync", 2)]
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} DETACH PARTITION '{partition_expr}'",
        settings=settings,
    )


@TestStep
def alter_drop_partition(
    self, table_name, partition_expr="10", node=None, settings=[("mutations_sync", 2)]
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} DROP PARTITION '{partition_expr}'", settings=settings
    )


@TestStep
def alter_drop_detached_partition(
    self, table_name, partition_expr, node=None, settings=[("mutations_sync", 2)]
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} DROP DETACHED PARTITION '{partition_expr}'",
        settings=settings,
    )


@TestStep
def alter_attach_partition(
    self, table_name, partition_expr="7", node=None, settings=[("mutations_sync", 2)]
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} ATTACH PARTITION '{partition_expr}'",
        settings=settings,
    )


@TestStep
def alter_replace_partition(
    self,
    table_name_2,
    table_name_1,
    partition_expr,
    node=None,
    settings=[("mutations_sync", 2)],
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name_2} REPLACE PARTITION '{partition_expr}' FROM {table_name_1}",
        settings=settings,
    )


@TestStep
def alter_fetch_partition(
    self, table_name, partition_expr, path, node=None, settings=[("mutations_sync", 2)]
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} FETCH PARTITION '{partition_expr}' FROM '{path}'",
        settings=settings,
    )


@TestStep
def alter_move_partition(
    self,
    table_name_1,
    table_name_2,
    partition_expr,
    node=None,
    settings=[("mutations_sync", 2)],
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name_1} MOVE PARTITION '{partition_expr}' to TABLE {table_name_2}",
        settings=settings,
    )


@TestStep
def alter_freeze_partition(
    self, table_name, partition_expr="9", node=None, settings=[("mutations_sync", 2)]
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} FREEZE PARTITION '{partition_expr}'",
        settings=settings,
    )


@TestStep
def alter_unfreeze_partition(
    self, table_name, partition_expr="9", node=None, settings=[("mutations_sync", 2)]
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} UNFREEZE PARTITION '{partition_expr}' WITH NAME 'backup_name'",
        settings=settings,
    )


@TestStep
def alter_update_in_partition(
    self,
    table_name,
    update_expr="x=10",
    partition_expr="WHERE x > -1",
    node=None,
    settings=[("mutations_sync", 2)],
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} UPDATE {update_expr} {partition_expr}",
        settings=settings,
    )


@TestStep
def alter_delete_in_partition(
    self,
    table_name,
    partition_expr="11",
    where_expr="WHERE x > -1",
    node=None,
    settings=[("mutations_sync", 2)],
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} DELETE IN PARTITION '{partition_expr}' {where_expr}",
        settings=settings,
    )


@TestStep
def alter_add_column(
    self,
    table_name,
    column_name="qkrq",
    column_type="Int32",
    default_expr="",
    node=None,
    settings=[("mutations_sync", 2)],
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type} {default_expr} FIRST ",
        settings=settings,
    )


@TestStep
def alter_drop_column(
    self, table_name, column_name="qkrq", node=None, settings=[("mutations_sync", 2)]
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name}",
        settings=settings,
    )


@TestStep
def alter_clear_column(
    self,
    table_name,
    column_name="qkrq",
    partition_expr="",
    node=None,
    settings=[("mutations_sync", 2)],
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} CLEAR COLUMN IF EXISTS {column_name} {partition_expr}",
        settings=settings,
    )


@TestStep
def alter_comment_column(
    self,
    table_name,
    column_name="x",
    column_comment="some comment",
    node=None,
    settings=[("mutations_sync", 2)],
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} COMMENT COLUMN {column_name} '{column_comment}'",
        settings=settings,
    )


@TestStep
def alter_modify_column(
    self,
    table_name,
    column_name="qkrq",
    column_type="Int32",
    default_expr="DEFAULT 48",
    node=None,
    settings=[("mutations_sync", 2)],
):
    if node is None:
        node = self.context.node

    node.query(
        f"ALTER TABLE {table_name} MODIFY COLUMN IF EXISTS {column_name} {column_type} {default_expr}",
        settings=settings,
    )


@TestStep(Then)
def detach_partition_after_delete_in_the_table(
    self,
    table_name,
    partitions=10,
    partition_expr="3",
    nodes=["clickhouse1"],
    node=None,
):
    """Check that detach partition after lightweight delete perform correctly when table already exists."""

    if node is None:
        node = self.context.node

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0 or id = {partition_expr})"
        ).output

    with Then("I delete odd rows from the table"):
        delete_odd(num_partitions=partitions, table_name=table_name)

    with Then("I detach third partition"):
        alter_detach_partition(
            table_name=table_name, partition_expr=partition_expr, node=node
        )

    with Then("I check that data is deleted from all nodes"):
        for name in nodes:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestStep(Then)
def drop_partition_after_delete_in_the_table(
    self,
    table_name,
    partitions=10,
    partition_expr="3",
    nodes=["clickhouse1"],
    node=None,
):
    """Check that drop partition after lightweight delete perform correctly when table already exists."""
    if node is None:
        node = self.context.node

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0 or id = {partition_expr})"
        ).output

    with Then("I delete odd rows from the table"):
        delete_odd(num_partitions=partitions, table_name=table_name)

    with Then("I drop third partition"):
        alter_drop_partition(
            table_name=table_name, partition_expr=partition_expr, node=node
        )

    with Then("I check that data is deleted from all nodes"):
        for name in nodes:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestStep(Then)
def attach_partition_after_delete_in_the_table(
    self,
    table_name,
    partitions=10,
    partition_expr="3",
    nodes=["clickhouse1"],
    node=None,
):
    """Check that attach partition after lightweight delete perform correctly when table already exists."""
    if node is None:
        node = self.context.node

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0 and id != {partition_expr})"
        ).output

    with When("I detach partition"):
        alter_detach_partition(
            table_name=table_name, partition_expr=partition_expr, node=node
        )

    with Then("I delete odd rows from the table"):
        delete_odd(num_partitions=partitions, table_name=table_name)

    with Then("I attach third partition"):
        alter_attach_partition(
            table_name=table_name, partition_expr=partition_expr, node=node
        )

    with Then("I check that data is deleted from all nodes"):
        for name in nodes:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestStep(Then)
def freeze_partition_after_delete_in_the_table(
    self,
    table_name,
    partitions=10,
    partition_expr="3",
    nodes=["clickhouse1"],
    node=None,
):
    """Check that freeze partition after lightweight delete perform correctly when table already exists."""
    if node is None:
        node = self.context.node

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with Then("I delete odd rows from the table"):
        delete_odd(num_partitions=partitions, table_name=table_name)

    with Then("I freeze third partition"):
        alter_freeze_partition(
            table_name=table_name, partition_expr=partition_expr, node=node
        )

    with Then("I check that data is deleted from all nodes"):
        for name in nodes:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestStep(Then)
def unfreeze_partition_after_delete_in_the_table(
    self,
    table_name,
    partitions=10,
    partition_expr="3",
    nodes=["clickhouse1"],
    node=None,
):
    """Check that unfreeze partition after lightweight delete perform correctly when table already exists."""
    if node is None:
        node = self.context.node

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with Then("I freeze third partition"):
        alter_freeze_partition(
            table_name=table_name, partition_expr=partition_expr, node=node
        )

    with Then("I delete odd rows from the table"):
        delete_odd(num_partitions=partitions, table_name=table_name)

    with Then("I unfreeze third partition"):
        alter_unfreeze_partition(
            table_name=table_name, partition_expr=partition_expr, node=node
        )

    with Then("I check that data is deleted from all nodes"):
        for name in nodes:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestStep(Then)
def add_column_after_delete_in_the_table(
    self,
    table_name,
    partitions=10,
    column_name="added_column",
    column_type="UInt32",
    default_expr="DEFAULT 7",
    output="500",
    nodes=["clickhouse1"],
    node=None,
):
    """Check that add column after lightweight delete perform correctly when table already exists."""
    if node is None:
        node = self.context.node

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with When("I delete odd rows from the table"):
        delete_odd(num_partitions=partitions, table_name=table_name)

    with And("I add column", description="name=added_column type=UInt32 default=7"):
        alter_add_column(
            table_name=table_name,
            column_name=column_name,
            column_type=column_type,
            default_expr=default_expr,
            node=node,
        )

    with Then("I check that data is deleted from all nodes"):
        for name in nodes:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestStep(Then)
def drop_column_after_delete_in_the_table(
    self, table_name, partitions=10, column_name="x", nodes=["clickhouse1"], node=None
):
    """Check that drop column after lightweight delete perform correctly when table already exists."""
    if node is None:
        node = self.context.node

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with When("I delete odd rows from the table"):
        delete_odd(num_partitions=partitions, table_name=table_name)

    with And("I drop column", description="name=x"):
        alter_drop_column(table_name=table_name, column_name=column_name, node=node)

    with Then("I check that data is deleted from all nodes"):
        for name in nodes:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestStep(Then)
def clear_column_after_delete_in_the_table(
    self, table_name, partitions=10, column_name="x", nodes=["clickhouse1"], node=None
):
    """Check that clear column after lightweight delete perform correctly when table already exists."""
    if node is None:
        node = self.context.node

    with When("I compute expected output"):
        output1 = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0 or id != 0)"
        ).output
        output2 = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with When("I delete odd rows from the table"):
        delete_odd(num_partitions=partitions, table_name=table_name)

    with And("I clear column", description="name=x"):
        alter_clear_column(
            table_name=table_name,
            column_name=column_name,
            partition_expr="IN PARTITION 0",
            node=node,
        )

    with Then("I check that data is deleted from all nodes"):
        for name in nodes:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count() FROM {table_name} where x=0")
                assert r.output == output1, error()
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output2, error()


@TestStep(Then)
def modify_column_after_delete_in_the_table(
    self,
    table_name,
    partitions=10,
    column_name="x",
    column_type="String",
    default_expr="DEFAULT '777'",
    nodes=["clickhouse1"],
    node=None,
):
    """Check that modify column after lightweight delete perform correctly when table already exists."""
    if node is None:
        node = self.context.node

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with When("I delete odd rows from the table"):
        delete_odd(num_partitions=partitions, table_name=table_name)

    with And(
        "I modify column",
        description=f"name={column_name}, type={column_type}, {default_expr}",
    ):
        alter_modify_column(
            table_name=table_name,
            column_name=column_name,
            column_type=column_type,
            default_expr=default_expr,
            node=node,
        )

    with Then("I check that data is deleted from all nodes"):
        for name in nodes:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestStep(Then)
def comment_column_after_delete_in_the_table(
    self,
    table_name,
    partitions=10,
    column_name="x",
    column_comment="hello",
    nodes=["clickhouse1"],
    node=None,
):
    """Check that comment column after lightweight delete perform correctly when table already exists."""

    if node is None:
        node = self.context.node

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with When("I delete odd rows from the table"):
        delete_odd(num_partitions=partitions, table_name=table_name)

    with And("I comment column", description="name=x, type=String, DEFAULT '777'"):
        alter_comment_column(
            table_name=table_name,
            column_name=column_name,
            column_comment=column_comment,
            node=node,
        )

    with Then("I check that data is deleted from all nodes"):
        for name in nodes:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully deleted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestStep
def delete_in_loop(self, table_name, condition, partitions=10, node=None):
    """Delete in loop by partitions."""
    for partition in range(partitions):
        delete(
            table_name=table_name,
            condition=condition + f" AND id = {partition}",
            node=node,
        )


@TestStep(When)
def create_and_check_replicated_table(
    self, table_name, partitions=10, parts_per_partition=1, block_size=100
):
    """Create replicated table with 3 replicas and insert into it."""
    create_replicated_table(table_name=table_name)

    with Then("I expect i can select from replicated table on any node"):
        for name in self.context.cluster.nodes["clickhouse"]:
            node = self.context.cluster.node(name)
            self.context.node = node
            with Then(f"I expect data is successfully inserted on {name} node"):
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == str(partitions * block_size), error()


@TestStep
def clear_update_in_loop(
    self, table_name, column_name, iterations=10, delay=0.6, node=None
):
    """Run clear column update column statements in a loop."""
    for i in range(iterations):
        alter_clear_column(table_name=table_name, column_name=column_name, node=node)
        time.sleep(delay / 2)
        alter_update_in_partition(
            table_name=table_name, update_expr=f"{column_name} = 777", node=node
        )
        time.sleep(delay / 2)


@TestStep
def modify_column_in_loop(
    self, table_name, column_name, iterations=10, delay=0.6, node=None
):
    """Run modify column statements in a loop."""
    for i in range(iterations):
        alter_modify_column(
            table_name=table_name,
            column_name=column_name,
            column_type="String",
            node=node,
        )
        time.sleep(delay / 2)
        alter_modify_column(
            table_name=table_name,
            column_name=column_name,
            column_type="Int32",
            node=node,
        )
        time.sleep(delay / 2)


@TestStep
def add_drop_column_in_loop(
    self,
    table_name,
    column_name,
    column_type,
    default_expr,
    iterations=10,
    delay=0.6,
    node=None,
):
    """Run add column drop column statements in a loop."""
    for i in range(iterations):
        alter_add_column(
            table_name=table_name,
            column_name=column_name,
            column_type=column_type,
            default_expr=default_expr,
            node=node,
        )
        time.sleep(delay / 2)
        alter_drop_column(table_name=table_name, column_name=column_name, node=node)
        time.sleep(delay / 2)


@TestStep
def attach_detach_in_loop(
    self, table_name, partition_expr, iterations=10, delay=0.6, node=None
):
    """Run detach attach statements in a loop for given partition expression."""
    for i in range(iterations):
        alter_detach_partition(
            table_name=table_name, partition_expr=partition_expr, node=node
        )
        time.sleep(delay / 2)
        alter_attach_partition(
            table_name=table_name, partition_expr=partition_expr, node=node
        )
        time.sleep(delay / 2)


@TestStep(Then)
def check_query_on_all_nodes(self, query, output):
    """Run query on all nodes and compare its result with `output`"""
    for name in self.context.cluster.nodes["clickhouse"]:
        node = self.context.cluster.node(name)
        self.context.node = node
        with Then(f"I expect data is correct on {name} node"):
            r = node.query(query)
            assert r.output == output, error()


@TestStep(When)
def create_table(self, table_name, table_engine=None, settings="", options=""):
    """Create table with engine specifying."""
    if table_engine is None:
        table_engine = self.context.table_engine

    if table_engine == "MergeTree":
        create_partitioned_table(
            table_name=table_name, settings=settings, options=options
        )

    if table_engine == "ReplacingMergeTree":
        create_replacing_merge_tree_table(
            table_name=table_name, settings=settings, options=options
        )

    if table_engine == "SummingMergeTree":
        create_summing_merge_tree_table(
            table_name=table_name, settings=settings, options=options
        )

    if table_engine == "AggregatingMergeTree":
        create_aggregating_merge_tree_table(
            table_name=table_name, settings=settings, options=options
        )

    if table_engine == "CollapsingMergeTree":
        create_collapsing_merge_tree_table(
            table_name=table_name, settings=settings, options=options
        )

    if table_engine == "VersionedCollapsingMergeTree":
        create_versioned_collapsing_merge_tree_table(
            table_name=table_name, settings=settings, options=options
        )

    if table_engine == "GraphiteMergeTree":
        create_graphite_merge_tree_table(
            table_name=table_name, settings=settings, options=options
        )

    if table_engine == "ReplicatedMergeTree":
        create_replicated_table(table_name=table_name)

    if table_engine == "ReplicatedReplacingMergeTree":
        create_replicated_replacing_merge_tree_table(table_name=table_name)

    if table_engine == "ReplicatedSummingMergeTree":
        create_replicated_summing_merge_tree_table(table_name=table_name)

    if table_engine == "ReplicatedAggregatingMergeTree":
        create_replicated_aggregating_merge_tree_table(table_name=table_name)

    if table_engine == "ReplicatedCollapsingMergeTree":
        create_replicated_collapsing_merge_tree_table(table_name=table_name)

    if table_engine == "ReplicatedVersionedCollapsingMergeTree":
        create_replicated_versioned_collapsing_merge_tree_table(table_name=table_name)

    if table_engine == "ReplicatedGraphiteMergeTree":
        create_replicated_graphite_merge_tree_table(table_name=table_name)


@TestStep
def delete_query_1_ontime(self, node=None):
    """Deleting All Rows In a Single Partition."""
    if node is None:
        node = self.context.node

    delete(table_name="datasets.ontime", condition="WHERE Year = 1990")


@TestStep
def delete_query_2_ontime(self, node=None):
    """Delete All Rows In Various Partitions."""
    if node is None:
        node = self.context.node

    delete(table_name="datasets.ontime", condition="WHERE Year % 2 = 0")


@TestStep
def delete_query_3_ontime(self, node=None):
    """Delete Some Rows In All Partitions (Large Granularity)."""
    if node is None:
        node = self.context.node

    delete(table_name="datasets.ontime", condition="WHERE Month = 2")


@TestStep
def delete_query_4_ontime(self, node=None):
    """Delete Some Rows In All Partitions (Small Granularity)."""
    if node is None:
        node = self.context.node

    delete(table_name="datasets.ontime", condition="WHERE DayofMonth = 2")


@TestStep
def delete_query_5_ontime(self, node=None):
    """Delete Some Rows In One Partition (Very Small Granularity)."""
    if node is None:
        node = self.context.node

    delete(table_name="datasets.ontime", condition="WHERE FlightDate = '2020-01-01'")


@TestStep
def insert_query_ontime(self, number=3, node=None):
    """Insert into ontime table."""
    if node is None:
        node = self.context.node

    node.query(
        f"INSERT INTO datasets.ontime SELECT * FROM datasets.ontime WHERE Month = {number}"
    )


@TestStep
def select_query_ontime(self, node=None):
    """Select from ontime table."""
    if node is None:
        node = self.context.node

    node.query(
        f"SELECT count(*) FROM (SELECT avg(c1) FROM (SELECT Year, Month, count(*) AS c1 FROM datasets.ontime GROUP BY Year, Month))"
    )


@TestStep(Given)
def create_view(self, view_type, view_name, condition, node=None):
    """Create view."""
    if node is None:
        node = self.context.node

    try:
        with Given("I create view"):
            if view_type == "LIVE":
                node.query(
                    f"CREATE {view_type} VIEW {view_name} as {condition}",
                    settings=[("allow_experimental_live_view", 1)],
                )
            elif view_type == "WINDOW":
                node.query(
                    f"CREATE {view_type} VIEW {view_name} as {condition}",
                    settings=[("allow_experimental_window_view", 1)],
                )
            else:
                node.query(f"CREATE {view_type} VIEW {view_name} as {condition}")

        yield
    finally:
        with Finally("I delete view"):
            node.query(f"DROP VIEW {view_name} SYNC")
