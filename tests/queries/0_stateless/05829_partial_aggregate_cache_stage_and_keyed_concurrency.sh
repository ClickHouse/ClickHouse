#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
# no-parallel: Messes with the global partial aggregate cache.
# no-random-* / no-parallel-replicas: settings randomization changes whether the cache is used at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="use_partial_aggregate_cache = 1, optimize_aggregation_in_order = 0, max_rows_to_group_by = 0, group_by_overflow_mode = 'throw'"

$CLICKHOUSE_CLIENT -q "SYSTEM DROP AGGREGATE CACHE"

# A query at the `with_mergeable_state` stage aggregates with `AggregatingTransform` and populates the cache. The
# identical query at the complete stage uses the radix partitioned `uniqExact`, which does not consume plan-time hits,
# so it must not remove the cached parts from the read.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_pac_radix;
    CREATE TABLE t_pac_radix (v UInt64) ENGINE = MergeTree ORDER BY v;
    SYSTEM STOP MERGES t_pac_radix;
    INSERT INTO t_pac_radix SELECT number FROM numbers(1000);
    INSERT INTO t_pac_radix SELECT number + 500 FROM numbers(1000);
"
$CLICKHOUSE_CLIENT --stage with_mergeable_state --format Null -q "SELECT uniqExact(v) FROM t_pac_radix SETTINGS $SETTINGS"
$CLICKHOUSE_CLIENT -q "SELECT uniqExact(v) FROM t_pac_radix SETTINGS $SETTINGS, optimize_uniq_exact_radix_partitioning = 1"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_pac_radix"

# Concurrent queries merge the same cached keyed `uniqExact` states that are large enough to be two-level sets.
# The keyed merge must not convert the shared source states in place, otherwise the queries race on them
# (detected by TSan) or return wrong results.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_pac_keyed;
    CREATE TABLE t_pac_keyed (k UInt8, x UInt64) ENGINE = MergeTree ORDER BY x;
    SYSTEM STOP MERGES t_pac_keyed;
    INSERT INTO t_pac_keyed SELECT 1, number FROM numbers(150000);
    INSERT INTO t_pac_keyed SELECT 1, number + 150000 FROM numbers(150000);
"

for _ in {1..6}
do
    $CLICKHOUSE_CLIENT -q "SELECT k, uniqExact(x) FROM t_pac_keyed GROUP BY k SETTINGS $SETTINGS, max_threads = 4" &
done
wait

$CLICKHOUSE_CLIENT -q "SELECT k, uniqExact(x) FROM t_pac_keyed GROUP BY k SETTINGS $SETTINGS, max_threads = 4"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_pac_keyed"
