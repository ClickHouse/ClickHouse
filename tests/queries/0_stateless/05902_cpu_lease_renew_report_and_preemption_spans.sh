#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the MASTER THREAD CPU resource is system-global, the server allows only one at a time

# With the `cpu_slot_preemption` server setting (on by default) "every thread renews its CPU slot
# periodically (according to `cpu_slot_quantum_ns` server setting). Such a renewal can block
# execution if CPU is overloaded" -- docs/concepts/features/configuration/server-config/
# workload-scheduling.mdx, "Preemption and down-scaling". The session setting
# `opentelemetry_trace_cpu_scheduling` is documented as "Collect OpenTelemetry spans for workload
# preemptive CPU scheduling" (src/Core/Settings.cpp): a renewal that reports consumed CPU time
# emits a CPU_LEASE_REPORT span, and a renewal that has to wait for the next slot emits a
# CPU_LEASE_PREEMPTION span.
#
# The workload below is what makes the renewals block. Its token bucket allows
# "max_cpus * delta + max_burst_cpu_seconds CPU seconds" of CPU in any interval of `delta` seconds
# (same document, "CPU throttling"). With max_cpus = 0.2 and no burst, the single query thread
# wants a whole core but is granted a fifth of one, so after the first quanta it is always ahead of
# its budget and the renewals have to wait for the bucket to refill.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    DROP WORKLOAD IF EXISTS 05902_wl;
    DROP RESOURCE IF EXISTS 05902_cpu;
    CREATE RESOURCE 05902_cpu (MASTER THREAD, WORKER THREAD);
    CREATE WORKLOAD 05902_wl SETTINGS max_cpus = 0.2, max_burst_cpu_seconds = 0;
"

# Own trace, so that only the spans of the query below are counted.
trace_id=$($CLICKHOUSE_CLIENT -q "SELECT lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
span_filter="finish_date >= yesterday() AND trace_id = UUIDNumToString(toFixedString(unhex('$trace_id'), 16))"

# `sipHash64(number) % 2` is 0 or 1 for every row, so the count is the number of rows read, and the
# hash of every row is the CPU time that the renewals report and that the workload throttles.
$CLICKHOUSE_CLIENT --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --workload=05902_wl --max_threads=1 --use_concurrency_control=1 \
    --opentelemetry_trace_cpu_scheduling=1 \
    -q "SELECT count() FROM numbers(8000000) WHERE sipHash64(number) % 2 IN (0, 1)"

# The spans are written by the query threads and flushed by a background thread; poll for them.
poll_delay=0.3
for _ in {1..20}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS opentelemetry_span_log"
    both=$($CLICKHOUSE_CLIENT -q "
        SELECT countIf(operation_name = 'CPU_LEASE_REPORT') > 0
           AND countIf(operation_name = 'CPU_LEASE_PREEMPTION') > 0
        FROM system.opentelemetry_span_log WHERE $span_filter")
    [[ "$both" == 1 ]] && break
    sleep "$poll_delay"
done

$CLICKHOUSE_CLIENT -q "
    SELECT
        countIf(operation_name = 'CPU_LEASE_REPORT') > 0 AS renewal_reported_cpu_time,
        countIf(operation_name = 'CPU_LEASE_PREEMPTION') > 0 AS renewal_waited_for_a_slot
    FROM system.opentelemetry_span_log WHERE $span_filter
    FORMAT TSV"

$CLICKHOUSE_CLIENT -nm -q "
    DROP WORKLOAD 05902_wl;
    DROP RESOURCE 05902_cpu;
"
