#!/usr/bin/env bash
# Tags: no-darwin, no-msan, no-flaky-check
# no-darwin: on Darwin StackTrace::resolveAddress keeps runtime addresses (AddressKind::Unsupported).
# no-msan: the query profiler is not supported in MemorySanitizer builds.
# no-flaky-check: flushing `trace_log` with every allocation sampled is too slow when run many times in parallel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"

$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT sum(sleepEachRow(0.05)) FROM numbers(20)
    SETTINGS query_profiler_real_time_period_ns = 10000000, max_block_size = 1,
        memory_profiler_sample_probability = 1, max_untracked_memory = 0, log_processors_profiles = 1
    FORMAT Null"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS system.trace_log, system.processors_profile_log"

# `trace` holds no runtime addresses, `ptr` is a keyed hash (heap addresses are below 2^48).
$CLICKHOUSE_CLIENT -q "
    SELECT
        countIf(trace_type = 'Real' AND arrayExists(x -> x > (SELECT max(address_end) FROM system.symbols), trace)) = 0,
        countIf(trace_type = 'MemorySample' AND ptr >= pow(2, 48)) > 0
    FROM system.trace_log
    WHERE event_date >= yesterday() AND query_id = '$query_id'
    SETTINGS allow_introspection_functions = 1"

# Processor and plan step ids are small indices, not heap addresses.
$CLICKHOUSE_CLIENT -q "
    SELECT max(id) < 100000, max(plan_step) < 100000, countIf(plan_step != 0) > 0
    FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND query_id = '$query_id'"
