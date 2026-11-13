#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Boundary cases that previously wrapped around an unsigned integer (which UBSan now reports).

echo "-- grouped fsync of an insert hashes the file identity"
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_grouped_fsync;
CREATE TABLE t_grouped_fsync (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS fsync_after_insert = 1, group_fsync_before_insert_finish = 1;
INSERT INTO t_grouped_fsync VALUES (1);
SELECT count() FROM t_grouped_fsync;
DROP TABLE t_grouped_fsync;"

echo "-- second interval rounding before the epoch in a fractional-hour time zone"
$CLICKHOUSE_CLIENT -q "
SELECT toStartOfInterval(toDateTime(100, 'Asia/Kathmandu'), INTERVAL 7 SECOND) AS t, toUnixTimestamp(t)"

echo "-- windowFunnel with the maximum window"
$CLICKHOUSE_CLIENT -q "
SELECT windowFunnel(18446744073709551615)(number + 1, number = 0, number = 1) FROM numbers(2) SETTINGS max_threads = 1;
SELECT windowFunnel(18446744073709551615, 'strict_once')(number + 1, number = 0, number = 1) FROM numbers(2) SETTINGS max_threads = 1;
SELECT windowFunnel(1)(number + 1, number = 0, number = 1, number = 3) FROM numbers(4) SETTINGS max_threads = 1;
SELECT windowFunnel(1, 'strict_once')(number + 1, number = 0, number = 1, number = 3) FROM numbers(4) SETTINGS max_threads = 1;"

echo "-- memory estimate of the sorter with the maximum block size"
$CLICKHOUSE_CLIENT -q "
SELECT groupArray(number) FROM (
    SELECT number FROM numbers(3) ORDER BY number % 2, number
    SETTINGS max_threads = 1, max_block_size = 18446744073709551615, enable_adaptive_memory_spill_scheduler = 1,
        max_memory_usage = 1073741824)"

echo "-- count of Npy rows with the maximum block size"
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$CUR_DIR/data_npy/one_dim.npy', Npy, 'x Int64')
SETTINGS max_threads = 1, max_block_size = 18446744073709551615, use_cache_for_count_from_files = 0"

echo "-- empty 'medium' state of quantilesTiming"
$CLICKHOUSE_CLIENT -q "
SELECT finalizeAggregation(CAST(unhex('020000000000000000'), 'AggregateFunction(quantilesTiming(1), UInt64)'))" 2>&1 \
    | grep -o -m1 'INCORRECT_DATA'
$CLICKHOUSE_CLIENT -q "
SELECT finalizeAggregation(CAST(unhex('020000000000000000'), 'AggregateFunction(quantileTiming(0.5), UInt64)'))" 2>&1 \
    | grep -o -m1 'INCORRECT_DATA'
