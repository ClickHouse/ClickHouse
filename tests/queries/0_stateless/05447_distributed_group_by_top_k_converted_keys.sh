#!/usr/bin/env bash

# `GROUP BY ... LIMIT` without `ORDER BY` over a `Distributed` table: the shards keep their smallest keys only if
# their keys convert to the declared types without merging or reordering. Here `'0001'`, `'001'`, `'01'` and `'1'`
# are distinct `String` keys on a shard and all become `1` as `UInt64`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The initiator merges two-level states of the shards bucket by bucket, and the converted keys of one group can come in
# different buckets, so it leaves duplicate groups whether the shards prune or not.
SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 0, prefer_localhost_replica = 0,
    enable_group_by_top_k_optimization = 1, max_rows_to_group_by = 0, exact_rows_before_limit = 0,
    query_plan_max_limit_for_top_k_optimization = 1000, optimize_aggregation_in_order = 0,
    group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_05447_string (k String) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_05447_string SELECT arrayJoin(['0001', '001', '01', '1', '2', '3', '4']);
    CREATE TABLE t_05447_string_as_uint (k UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_05447_string);
    CREATE TABLE t_05447_string_as_string AS t_05447_string
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_05447_string);
    CREATE TABLE t_05447_uint (k UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_05447_uint SELECT arrayJoin([1, 2, 10, 11]);
    CREATE TABLE t_05447_uint_as_string (k String)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_05447_uint);
"

function sorts_for_top_k()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT count() FROM (EXPLAIN SELECT k, count() FROM $1 GROUP BY k LIMIT 2 SETTINGS $SETTINGS)
        WHERE explain LIKE '%Sorting for GROUP BY top-K%'"
}

echo "-- The shards keep all their groups when the conversion merges or reorders the keys."
$CLICKHOUSE_CLIENT -q "SELECT k, count() FROM t_05447_string_as_uint GROUP BY k LIMIT 4 SETTINGS $SETTINGS" | sort
sorts_for_top_k t_05447_string_as_uint
sorts_for_top_k t_05447_uint_as_string
echo "-- Without a conversion they keep their smallest keys."
sorts_for_top_k t_05447_string_as_string

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_05447_string_as_uint;
    DROP TABLE t_05447_string_as_string;
    DROP TABLE t_05447_uint_as_string;
    DROP TABLE t_05447_string;
    DROP TABLE t_05447_uint;
"
