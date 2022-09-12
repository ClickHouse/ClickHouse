from collections import namedtuple

table_tuple = namedtuple("table_tuple", "create_statement cluster")

table_types = {
    "MergeTree": table_tuple(
        "CREATE TABLE {name} (d DATE, a String, b UInt8, x String, y Int8) ENGINE = MergeTree() PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        None,
    ),
    "ReplacingMergeTree": table_tuple(
        "CREATE TABLE {name} (d DATE, a String, b UInt8, x String, y Int8) ENGINE = ReplacingMergeTree() PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        None,
    ),
    "SummingMergeTree": table_tuple(
        "CREATE TABLE {name} (d DATE, a String, b UInt8 DEFAULT 1, x String, y Int8) ENGINE = SummingMergeTree() PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        None,
    ),
    "AggregatingMergeTree": table_tuple(
        "CREATE TABLE {name} (d DATE, a String, b UInt8, x String, y Int8) ENGINE = AggregatingMergeTree() PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        None,
    ),
    "CollapsingMergeTree": table_tuple(
        "CREATE TABLE {name} (d Date, a String, b UInt8, x String, y Int8, sign Int8 DEFAULT 1) ENGINE = CollapsingMergeTree(sign) PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        None,
    ),
    "VersionedCollapsingMergeTree": table_tuple(
        "CREATE TABLE {name} (d Date, a String, b UInt8, x String, y Int8, version UInt64, sign Int8 DEFAULT 1) ENGINE = VersionedCollapsingMergeTree(sign, version) PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        None,
    ),
    "GraphiteMergeTree": table_tuple(
        "CREATE TABLE {name} (d Date, a String, b UInt8, x String, y Int8, Path String, Time DateTime, Value Float64, col UInt64, Timestamp Int64) ENGINE = GraphiteMergeTree('graphite_rollup_example') PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        None,
    ),
    "ReplicatedMergeTree-sharded_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER sharded_cluster (d DATE, a String, b UInt8, x String, y Int8) \
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "sharded_cluster",
    ),
    "ReplicatedMergeTree-one_shard_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER one_shard_cluster (d DATE, a String, b UInt8, x String, y Int8) \
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "one_shard_cluster",
    ),
    "ReplicatedReplacingMergeTree-sharded_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER sharded_cluster (d DATE, a String, b UInt8, x String, y Int8) \
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "sharded_cluster",
    ),
    "ReplicatedReplacingMergeTree-one_shard_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER one_shard_cluster (d DATE, a String, b UInt8, x String, y Int8) \
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "one_shard_cluster",
    ),
    "ReplicatedSummingMergeTree-sharded_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER sharded_cluster (d DATE, a String, b UInt8 DEFAULT 1, x String, y Int8) \
        ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "sharded_cluster",
    ),
    "ReplicatedSummingMergeTree-one_shard_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER one_shard_cluster (d DATE, a String, b UInt8 DEFAULT 1, x String, y Int8) \
        ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "one_shard_cluster",
    ),
    "ReplicatedAggregatingMergeTree-sharded_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER sharded_cluster (d DATE, a String, b UInt8, x String, y Int8) \
        ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "sharded_cluster",
    ),
    "ReplicatedAggregatingMergeTree-one_shard_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER one_shard_cluster (d DATE, a String, b UInt8, x String, y Int8) \
        ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "one_shard_cluster",
    ),
    "ReplicatedCollapsingMergeTree-sharded_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER sharded_cluster (d Date, a String, b UInt8, x String, y Int8, sign Int8 DEFAULT 1) \
        ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', sign) PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "sharded_cluster",
    ),
    "ReplicatedCollapsingMergeTree-one_shard_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER one_shard_cluster (d Date, a String, b UInt8, x String, y Int8, sign Int8 DEFAULT 1) \
        ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', sign) PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "one_shard_cluster",
    ),
    "ReplicatedVersionedCollapsingMergeTree-sharded_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER sharded_cluster (d Date, a String, b UInt8, x String, y Int8, version UInt64, sign Int8 DEFAULT 1) \
        ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', sign, version) PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "sharded_cluster",
    ),
    "ReplicatedVersionedCollapsingMergeTree-one_shard_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER one_shard_cluster (d Date, a String, b UInt8, x String, y Int8, version UInt64, sign Int8 DEFAULT 1) \
        ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', sign, version) PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "one_shard_cluster",
    ),
    "ReplicatedGraphiteMergeTree-sharded_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER sharded_cluster (d Date, a String, b UInt8, x String, y Int8, Path String, Time DateTime, Value Float64, col UInt64, Timestamp Int64) \
        ENGINE = ReplicatedGraphiteMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', 'graphite_rollup_example') PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "sharded_cluster",
    ),
    "ReplicatedGraphiteMergeTree-one_shard_cluster": table_tuple(
        "CREATE TABLE {name} ON CLUSTER one_shard_cluster (d Date, a String, b UInt8, x String, y Int8, Path String, Time DateTime, Value Float64, col UInt64, Timestamp Int64) \
        ENGINE = ReplicatedGraphiteMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}', 'graphite_rollup_example') PARTITION BY y ORDER BY (b, d) PRIMARY KEY b",
        "one_shard_cluster",
    ),
}
