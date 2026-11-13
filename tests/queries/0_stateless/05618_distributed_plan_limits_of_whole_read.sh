#!/usr/bin/env bash

# A replica of a distributed plan reads only its bucket of a read. The limits that concern the whole
# read, `max_partitions_to_read` and the `max_concurrent_queries` admission of a read of at least
# `min_marks_to_honor_max_concurrent_queries` marks, are checked once, by the coordinator, as for the
# same query on a single node.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISTRIBUTED="make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_default_reader_bucket_count = 3, distributed_plan_fallback_to_local_execution = 0,
    enable_join_runtime_filters = 0, enable_parallel_replicas = 0, use_query_condition_cache = 0"

# `cityHash64` keeps the aggregation from being answered by statistics or a projection without a read.
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_dp_limits_partitions;
    CREATE TABLE t_dp_limits_partitions (p UInt8, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k
    SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi';
    SYSTEM STOP MERGES t_dp_limits_partitions;
    INSERT INTO t_dp_limits_partitions SELECT 0, number FROM numbers(30000);
    INSERT INTO t_dp_limits_partitions SELECT 1, number FROM numbers(30000);
    INSERT INTO t_dp_limits_partitions SELECT 2, number FROM numbers(30000);
"

echo "-- three partitions, max_partitions_to_read = 1: every bucket reads one, the read reads three"
$CLICKHOUSE_CLIENT -q "SELECT sum(cityHash64(k)) FROM t_dp_limits_partitions
    SETTINGS make_distributed_plan = 0, max_partitions_to_read = 1" 2>&1 | grep -o -m1 'TOO_MANY_PARTITIONS'
$CLICKHOUSE_CLIENT -q "SELECT sum(cityHash64(k)) FROM t_dp_limits_partitions
    SETTINGS $DISTRIBUTED, max_partitions_to_read = 1" 2>&1 | grep -o -m1 'TOO_MANY_PARTITIONS'
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(k)) = (SELECT sum(cityHash64(k)) FROM t_dp_limits_partitions SETTINGS make_distributed_plan = 0)
    FROM t_dp_limits_partitions SETTINGS $DISTRIBUTED, max_partitions_to_read = 3"

# 60 marks, a threshold of 31: the read counts, each of its buckets of 20 marks would not.
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_dp_limits_concurrency;
    CREATE TABLE t_dp_limits_concurrency (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi',
        max_concurrent_queries = 1, min_marks_to_honor_max_concurrent_queries = 31;
    INSERT INTO t_dp_limits_concurrency SELECT number FROM numbers(60000);
"

echo "-- the only slot is free: the read takes it once, not once per bucket"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(k)) = (SELECT sum(cityHash64(k)) FROM t_dp_limits_concurrency SETTINGS make_distributed_plan = 0)
    FROM t_dp_limits_concurrency SETTINGS $DISTRIBUTED"

echo "-- another read holds the only slot"
holder_query_id="${CLICKHOUSE_DATABASE}_slot_holder_$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$holder_query_id" -q "SELECT sum(cityHash64(k)) FROM t_dp_limits_concurrency WHERE NOT ignore(sleepEachRow(0.001))
    SETTINGS make_distributed_plan = 0, max_threads = 1, max_block_size = 100, function_sleep_max_microseconds_per_block = 0,
    use_query_condition_cache = 0" > /dev/null 2>&1 &
holder_pid=$!

# Wait until the holder reads, i.e. holds the slot.
while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$holder_query_id' AND read_rows > 0") == 0 ]]
do
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SELECT sum(cityHash64(k)) FROM t_dp_limits_concurrency
    SETTINGS make_distributed_plan = 0, use_query_condition_cache = 0" 2>&1 | grep -o -m1 'TOO_MANY_SIMULTANEOUS_QUERIES'
$CLICKHOUSE_CLIENT -q "SELECT sum(cityHash64(k)) FROM t_dp_limits_concurrency
    SETTINGS $DISTRIBUTED" 2>&1 | grep -o -m1 'TOO_MANY_SIMULTANEOUS_QUERIES'

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$holder_query_id' SYNC" > /dev/null
wait $holder_pid

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE t_dp_limits_concurrency;
    DROP TABLE t_dp_limits_partitions;
"
