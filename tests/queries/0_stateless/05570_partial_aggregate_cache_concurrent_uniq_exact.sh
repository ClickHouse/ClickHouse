#!/usr/bin/env bash

# Concurrent queries merge the same cached `uniqExact` states. Merging must not convert the shared source states in
# place (single-level to two-level set), otherwise the queries race on them (detected by TSan) or return wrong results.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_pac_concurrent;
    CREATE TABLE t_pac_concurrent (x UInt64) ENGINE = MergeTree ORDER BY x;
    SYSTEM STOP MERGES t_pac_concurrent;
    INSERT INTO t_pac_concurrent SELECT number FROM numbers(10000);
    INSERT INTO t_pac_concurrent SELECT number + 10000 FROM numbers(10000);
    INSERT INTO t_pac_concurrent SELECT number + 20000 FROM numbers(10000);
"

SETTINGS="use_partial_aggregate_cache = 1, optimize_aggregation_in_order = 0, max_rows_to_group_by = 0, group_by_overflow_mode = 'throw', max_threads = 4"

for _ in {1..8}
do
    $CLICKHOUSE_CLIENT -q "SELECT uniqExact(x) FROM t_pac_concurrent SETTINGS $SETTINGS" &
done
wait

$CLICKHOUSE_CLIENT -q "SELECT uniqExact(x) FROM t_pac_concurrent SETTINGS $SETTINGS"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_pac_concurrent"
