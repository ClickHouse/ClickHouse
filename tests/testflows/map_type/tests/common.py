import uuid
from collections import namedtuple

from testflows.core import *
from testflows.core.name import basename, parentname
from testflows._core.testtype import TestSubType

table_tuple = namedtuple("table_tuple", "create_statement cluster")

table_types = {
    "MergeTree": table_tuple("CREATE TABLE {name} (d DATE, a String, b UInt8, x String, m Map(String, Int8)) ENGINE = MergeTree() PARTITION BY m ORDER BY d", None),
    # "ReplacingMergeTree": table_tuple("CREATE TABLE {name} (d DATE, a String, b UInt8, x String, y Int8) ENGINE = ReplacingMergeTree() PARTITION BY y ORDER BY d", None),
    # "SummingMergeTree": table_tuple("CREATE TABLE {name} (d DATE, a String, b UInt8 DEFAULT 1, x String, y Int8) ENGINE = SummingMergeTree() PARTITION BY y ORDER BY d", None),
    # "AggregatingMergeTree": table_tuple("CREATE TABLE {name} (d DATE, a String, b UInt8, x String, y Int8) ENGINE = AggregatingMergeTree() PARTITION BY y ORDER BY d", None),
    # "CollapsingMergeTree": table_tuple("CREATE TABLE {name} (d Date, a String, b UInt8, x String, y Int8, sign Int8 DEFAULT 1) ENGINE = CollapsingMergeTree(sign) PARTITION BY y ORDER BY d", None),
    # "VersionedCollapsingMergeTree": table_tuple("CREATE TABLE {name} (d Date, a String, b UInt8, x String, y Int8, version UInt64, sign Int8 DEFAULT 1) ENGINE = VersionedCollapsingMergeTree(sign, version) PARTITION BY y ORDER BY d", None),
    # "GraphiteMergeTree": table_tuple("CREATE TABLE {name} (d Date, a String, b UInt8, x String, y Int8, Path String, Time DateTime, Value Float64, col UInt64, Timestamp Int64) ENGINE = GraphiteMergeTree('graphite_rollup_example') PARTITION BY y ORDER by d", None),
    # "ReplicatedMergeTree-sharded_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER sharded_cluster (d DATE, a String, b UInt8, x String, y Int8) \
    #     ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY d", "sharded_cluster"),
    # "ReplicatedMergeTree-one_shard_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER one_shard_cluster (d DATE, a String, b UInt8, x String, y Int8) \
    #     ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY d", "one_shard_cluster"),
    # "ReplicatedReplacingMergeTree-sharded_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER sharded_cluster (d DATE, a String, b UInt8, x String, y Int8) \
    #     ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY d", "sharded_cluster"),
    # "ReplicatedReplacingMergeTree-one_shard_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER one_shard_cluster (d DATE, a String, b UInt8, x String, y Int8) \
    #     ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY d", "one_shard_cluster"),
    # "ReplicatedSummingMergeTree-sharded_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER sharded_cluster (d DATE, a String, b UInt8 DEFAULT 1, x String, y Int8) \
    #     ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY d", "sharded_cluster"),
    # "ReplicatedSummingMergeTree-one_shard_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER one_shard_cluster (d DATE, a String, b UInt8 DEFAULT 1, x String, y Int8) \
    #     ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY d", "one_shard_cluster"),
    # "ReplicatedAggregatingMergeTree-sharded_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER sharded_cluster (d DATE, a String, b UInt8, x String, y Int8) \
    #     ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY d", "sharded_cluster"),
    # "ReplicatedAggregatingMergeTree-one_shard_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER one_shard_cluster (d DATE, a String, b UInt8, x String, y Int8) \
    #     ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY d", "one_shard_cluster"),
    # "ReplicatedCollapsingMergeTree-sharded_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER sharded_cluster (d Date, a String, b UInt8, x String, y Int8, sign Int8 DEFAULT 1) \
    #     ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', sign) PARTITION BY y ORDER BY d", "sharded_cluster"),
    # "ReplicatedCollapsingMergeTree-one_shard_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER one_shard_cluster (d Date, a String, b UInt8, x String, y Int8, sign Int8 DEFAULT 1) \
    #     ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', sign) PARTITION BY y ORDER BY d", "one_shard_cluster"),
    # "ReplicatedVersionedCollapsingMergeTree-sharded_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER sharded_cluster (d Date, a String, b UInt8, x String, y Int8, version UInt64, sign Int8 DEFAULT 1) \
    #     ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', sign, version) PARTITION BY y ORDER BY d", "sharded_cluster"),
    # "ReplicatedVersionedCollapsingMergeTree-one_shard_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER one_shard_cluster (d Date, a String, b UInt8, x String, y Int8, version UInt64, sign Int8 DEFAULT 1) \
    #     ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', sign, version) PARTITION BY y ORDER BY d", "one_shard_cluster"),
    # "ReplicatedGraphiteMergeTree-sharded_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER sharded_cluster (d Date, a String, b UInt8, x String, y Int8, Path String, Time DateTime, Value Float64, col UInt64, Timestamp Int64) \
    #     ENGINE = ReplicatedGraphiteMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', 'graphite_rollup_example') PARTITION BY y ORDER BY d", "sharded_cluster"),
    # "ReplicatedGraphiteMergeTree-one_shard_cluster": table_tuple("CREATE TABLE {name} ON CLUSTER one_shard_cluster (d Date, a String, b UInt8, x String, y Int8, Path String, Time DateTime, Value Float64, col UInt64, Timestamp Int64) \
    #     ENGINE = ReplicatedGraphiteMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', 'graphite_rollup_example') PARTITION BY y ORDER BY d", "one_shard_cluster"),
}

def getuid():
    if current().subtype == TestSubType.Example:
        testname = f"{basename(parentname(current().name)).replace(' ', '_').replace(',','')}"
    else:
        testname = f"{basename(current().name).replace(' ', '_').replace(',','')}"
    return testname + "_" + str(uuid.uuid1()).replace('-', '_')

@TestStep(Given)
def allow_experimental_map_type(self):
    """Set allow_experimental_map_type = 1
    """
    setting = ("allow_experimental_map_type", 1)
    default_query_settings = None

    try:
        with By("adding allow_experimental_map_type to the default query settings"):
            default_query_settings = getsattr(current().context, "default_query_settings", [])
            default_query_settings.append(setting)
        yield
    finally:
        with Finally("I remove allow_experimental_map_type from the default query settings"):
            if default_query_settings:
                try:
                    default_query_settings.pop(default_query_settings.index(setting))
                except ValueError:
                    pass

@TestStep(Given)
def create_table(self, name, statement, on_cluster=False):
    """Create table.
    """
    node = current().context.node
    try:
        with Given(f"I have a {name} table"):
            node.query(statement.format(name=name))
        yield name
    finally:
        with Finally("I drop the table"):
            if on_cluster:
                node.query(f"DROP TABLE IF EXISTS {name} ON CLUSTER {on_cluster}")
            else:
                node.query(f"DROP TABLE IF EXISTS {name}")