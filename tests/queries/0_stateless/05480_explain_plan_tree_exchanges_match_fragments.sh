#!/usr/bin/env bash
# `EXPLAIN PLAN tree = 1` and `EXPLAIN PLAN fragments = 1` print the same `Exchanges` and `Levels` sections: both are built
# from one prepared view of the fragment graph, so the exchange keys get the same names, also with `pretty` when the two
# sides of a join have columns of the same name. The queries are only explained, never executed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="
SET query_plan_join_swap_table = 0;
SET max_rows_to_group_by = 0;
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;
SET join_runtime_filter_min_probe_rows = 0, query_plan_optimize_join_order_randomize = 0, use_hash_table_stats_for_join_reordering = 0;
SET
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    enable_join_runtime_filters = 0,
    use_statistics = 0,
    distributed_plan_optimize_exchanges = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_force_exchange_kind = 'Streaming';
"

$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
CREATE TABLE l(k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r(k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO l SELECT number, number FROM numbers(100000);
INSERT INTO r SELECT number, number FROM numbers(1000);
EOF

# The sections are the tail of both outputs: everything from the `Exchanges:` line on.
sections()
{
    sed -n '/^Exchanges:$/,$p'
}

check()
{
    local name="$1" options="$2" query="$3"
    echo "-- $name"
    local fragments tree
    fragments=$($CLICKHOUSE_CLIENT --multiquery -q "$SETTINGS EXPLAIN fragments = 1$options $query" | sections)
    tree=$($CLICKHOUSE_CLIENT --multiquery -q "$SETTINGS EXPLAIN tree = 1$options $query" | sections)
    echo "$fragments"
    if [ "$fragments" == "$tree" ]; then
        echo "tree: same sections"
    else
        echo "tree: different sections"
        diff <(echo "$fragments") <(echo "$tree")
    fi
}

check "shuffle join, the key has the same name on both sides" "" \
    "SELECT l.k, sum(r.w) FROM l JOIN r ON l.k = r.k GROUP BY l.k SETTINGS distributed_plan_max_rows_to_broadcast = 0"

check "broadcast join" "" \
    "SELECT count() FROM l JOIN r ON l.k = r.k SETTINGS distributed_plan_max_rows_to_broadcast = 1000000"

check "aggregation" "" \
    "SELECT k % 10 AS g, sum(v) FROM l GROUP BY g"

check "order by, a merging gather" "" \
    "SELECT k FROM l ORDER BY k DESC"

check "shuffle join without pretty" ", pretty = 0" \
    "SELECT l.k, sum(r.w) FROM l JOIN r ON l.k = r.k GROUP BY l.k SETTINGS distributed_plan_max_rows_to_broadcast = 0"

$CLICKHOUSE_CLIENT -q "DROP TABLE l; DROP TABLE r" --multiquery
