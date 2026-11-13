#!/usr/bin/env bash

# A replica task of a distributed read reads only its bucket, but the `max_concurrent_queries` admission
# of the read must still count the marks of the whole read against `min_marks_to_honor_max_concurrent_queries`:
# 60 marks in three buckets of 20, with a threshold of 31. While another read holds the only slot, the
# distributed read is rejected, as the same read on a single node is.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `cityHash64` keeps the aggregation from being answered by statistics or a projection without a read.
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_dp_admission;
    CREATE TABLE t_dp_admission (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi',
        max_concurrent_queries = 1, min_marks_to_honor_max_concurrent_queries = 31;
    INSERT INTO t_dp_admission SELECT number FROM numbers(60000);
"

holder_query_id="${CLICKHOUSE_DATABASE}_slot_holder_$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$holder_query_id" -q "SELECT sum(cityHash64(k)) FROM t_dp_admission WHERE NOT ignore(sleepEachRow(0.001))
    SETTINGS make_distributed_plan = 0, max_threads = 1, max_block_size = 100, function_sleep_max_microseconds_per_block = 0,
    use_query_condition_cache = 0" > /dev/null 2>&1 &
holder_pid=$!

# Wait until the holder reads, i.e. holds the slot.
while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$holder_query_id' AND read_rows > 0") == 0 ]]
do
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SELECT sum(cityHash64(k)) FROM t_dp_admission
    SETTINGS make_distributed_plan = 0, use_query_condition_cache = 0" 2>&1 | grep -o -m1 'TOO_MANY_SIMULTANEOUS_QUERIES'
$CLICKHOUSE_CLIENT -q "SELECT sum(cityHash64(k)) FROM t_dp_admission
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_default_reader_bucket_count = 3, distributed_plan_fallback_to_local_execution = 0,
    enable_join_runtime_filters = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    max_rows_to_group_by = 0, use_query_condition_cache = 0" 2>&1 | grep -o -m1 'TOO_MANY_SIMULTANEOUS_QUERIES'

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$holder_query_id' SYNC" > /dev/null
wait "$holder_pid"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dp_admission"
