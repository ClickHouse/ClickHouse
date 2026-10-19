#!/usr/bin/env bash
# Tags: no-darwin
# no-darwin: on Darwin StackTrace::resolveAddress keeps runtime addresses (AddressKind::Unsupported).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"

# Profiler samples taken while sleeping in the C library, sampled allocations, and a pipeline with plan steps.
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT sum(x), count() FROM (SELECT number % 7 AS x, sleepEachRow(0.05) FROM numbers(20) GROUP BY x)
    SETTINGS query_profiler_real_time_period_ns = 10000000, max_block_size = 1,
        memory_profiler_sample_probability = 1, max_untracked_memory = 0, log_processors_profiles = 1
    FORMAT Null"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS system.trace_log, system.processors_profile_log"

# `trace` holds only offsets inside the main binary, frames outside it are 0 and keep their names in `symbols`.
$CLICKHOUSE_CLIENT -q "
    SELECT
        countIf(x BETWEEN 1 AND (SELECT max(address_end) FROM system.symbols)) > 0,
        countIf(x > (SELECT max(address_end) FROM system.symbols)) = 0,
        countIf(x = 0) > 0,
        countIf(x = 0 AND s != '') > 0
    FROM system.trace_log
    ARRAY JOIN trace AS x, symbols AS s
    WHERE event_date >= yesterday() AND query_id = '$query_id' AND trace_type = 'Real'
    SETTINGS allow_introspection_functions = 1"

# `ptr` is a keyed hash: heap addresses are below 2^48, the hash is not.
$CLICKHOUSE_CLIENT -q "
    SELECT countIf(ptr >= pow(2, 48)) > 0, countIf(ptr = 0) = 0
    FROM system.trace_log
    WHERE event_date >= yesterday() AND query_id = '$query_id' AND trace_type = 'MemorySample'"

# Processor and plan step ids are indices within the query, not heap addresses.
$CLICKHOUSE_CLIENT -q "
    WITH (SELECT groupArray(id) FROM system.processors_profile_log WHERE event_date >= yesterday() AND query_id = '$query_id') AS ids
    SELECT
        max(id) < 100000,
        max(plan_step) < 100000,
        countIf(plan_step != 0) > 0,
        uniqExact(id) = count(),
        sum(arrayCount(p -> NOT has(ids, p), parent_ids)) = 0
    FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND query_id = '$query_id'"
