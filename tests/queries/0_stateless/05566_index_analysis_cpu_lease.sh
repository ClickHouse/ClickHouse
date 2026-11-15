#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-flaky-check, no-replicated-database, no-parallel-replicas
# no-parallel: the server allows only one MASTER THREAD / WORKER THREAD resource at a time
# no-fasttest: multi-second index analyses
# no-flaky-check: with its thread fuzzer the inserts into the index_granularity = 1 tables alone exceed the timeout
# no-replicated-database: query_log lookups by query_id on one node
# no-parallel-replicas: parallel replicas move the index analysis to the replicas

# Index analysis of MergeTree runs under the workload's preemptive CPU scheduling: its CPU is reported to the
# workload, it is preempted inside a part (the primary key search, a skip index, the top-K path), it never hangs a
# one-slot workload (also not in a dictionary load), and KILL QUERY, a cancellation by the client or a soft time limit
# wakes it up.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

P="${CLICKHOUSE_DATABASE}_ia"
# Not per database: CREATE OR REPLACE then also replaces the resource of a killed run
RES="test_05566_ia_cpu"

cleanup() {
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id LIKE '${P}_%' SYNC FORMAT Null" 2>/dev/null
    for w in hi lo grp one cap wl all; do
        $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS ${P}_${w}" 2>/dev/null
    done
    $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS ${RES}" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS ${RES}_w" 2>/dev/null
}
trap cleanup EXIT

BIG_COND="b IN (1, 62501, 125001, 187501, 250001, 312501, 375001, 437501, 500001, 562501, 625001, 687501, 750001, 812501, 875001, 937501)"

$CLICKHOUSE_CLIENT -m -q "
    DROP RESOURCE IF EXISTS ${RES}_w;
    CREATE OR REPLACE RESOURCE ${RES} (MASTER THREAD, WORKER THREAD);
    CREATE WORKLOAD ${P}_all;
    CREATE WORKLOAD ${P}_wl IN ${P}_all;
    -- No burst, so a short analysis is throttled from its first quantum
    CREATE WORKLOAD ${P}_cap IN ${P}_all SETTINGS max_cpus = 0.5, max_burst_cpu_seconds = 0;
    CREATE WORKLOAD ${P}_one IN ${P}_all SETTINGS max_concurrent_threads = 1;
    CREATE WORKLOAD ${P}_grp IN ${P}_all SETTINGS max_concurrent_threads = 1;
    CREATE WORKLOAD ${P}_hi IN ${P}_grp SETTINGS priority = 0;
    CREATE WORKLOAD ${P}_lo IN ${P}_grp SETTINGS priority = 1;

    -- t, big and bs are on local disk: object storage reads spend CPU in reader threads that hold no lease, and they
    -- park the lease, which lets a capped analysis run past max_cpus without a preemption (#126133)

    -- With index_granularity = 1 and a filter on the second key column, the generic exclusion search dominates
    CREATE TABLE t (a UInt64, b UInt64, INDEX ib b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY (a, b)
        SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', storage_policy = 'default';
    SYSTEM STOP MERGES t;
    INSERT INTO t SELECT number % 25000, number FROM numbers(250000);
    INSERT INTO t SELECT number % 25000, number FROM numbers(250000);
    INSERT INTO t SELECT number % 25000, number FROM numbers(250000);
    INSERT INTO t SELECT number % 25000, number FROM numbers(250000);

    CREATE TABLE big (a UInt64, b UInt64, INDEX ib b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY (a, b)
        SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', storage_policy = 'default';
    INSERT INTO big SELECT number % 100000, number FROM numbers(1000000);

    -- b is even, so the source of the dictionary has a long analysis and reads nothing
    CREATE TABLE be (a UInt64, b UInt64, INDEX ib b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY (a, b)
        SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';
    INSERT INTO be SELECT number % 100000, number * 2 FROM numbers(1000000);
    CREATE DICTIONARY ${P}_dict (a UInt64, b UInt64) PRIMARY KEY a
        SOURCE(CLICKHOUSE(DB '${CLICKHOUSE_DATABASE}' TABLE 'be' WHERE '${BIG_COND}'))
        LAYOUT(HASHED()) LIFETIME(0)
        SETTINGS(use_query_condition_cache = 0, merge_tree_generic_exclusion_search_max_steps = 0, use_skip_indexes_on_data_read = 0);

    -- b is not in the sorting key, so the skip index dominates
    CREATE TABLE bs (a UInt64, b UInt64, INDEX ib b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', storage_policy = 'default';
    INSERT INTO bs SELECT number % 100000, number FROM numbers(1000000);

    CREATE TABLE p (k UInt64, v UInt64, INDEX iv v TYPE set(100) GRANULARITY 1) ENGINE = MergeTree PARTITION BY k % 8 ORDER BY k
        SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';
    INSERT INTO p SELECT number, number % 1000 FROM numbers(200000);

    CREATE TABLE pr (k UInt64, v UInt64, PROJECTION pv (SELECT * ORDER BY v)) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';
    SYSTEM STOP MERGES pr;
    INSERT INTO pr SELECT number, number % 1000 FROM numbers(100000);
    INSERT INTO pr SELECT number, number % 1000 FROM numbers(100000, 100000);
"

T_COND="b IN (1, 15626, 31251, 46876, 62501, 78126, 93751, 109376, 125001, 140626, 156251, 171876, 187501, 203126, 218751, 234376)"
COMMON="use_concurrency_control = 1, use_query_condition_cache = 0, merge_tree_generic_exclusion_search_max_steps = 0, use_skip_indexes_on_data_read = 0"

dequeued_cost() {
    $CLICKHOUSE_CLIENT -q "SELECT sum(dequeued_cost) FROM system.scheduler WHERE resource = '${RES}' AND type = 'workload' AND path LIKE '%/$1'"
}

echo "--- the CPU of the analysis is reported to the workload"
before=$(dequeued_cost "${P}_wl")
$CLICKHOUSE_CLIENT --query_id "${P}_accounting" -q "SELECT count() FROM t WHERE ${T_COND} SETTINGS workload = '${P}_wl', max_threads = 4, ${COMMON}"
after=$(dequeued_cost "${P}_wl")
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT ($after - $before) >= 0.5 * 1000 * (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${P}_accounting' AND type = 'QueryFinish'"

CAP="workload = '${P}_cap', max_threads = 1, ${COMMON}"
preempted_inside_part() {
    $CLICKHOUSE_CLIENT --query_id "$1" -q "$2"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT ProfileEvents['ConcurrencyControlPreemptions'] > 3, ProfileEvents['SelectedMarks'] < 1000
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$1' AND type = 'QueryFinish'"
}

echo "--- the analysis of one part is preempted inside the part"
preempted_inside_part "${P}_preempt" "SELECT count() FROM big WHERE ${BIG_COND} SETTINGS ${CAP}"
preempted_inside_part "${P}_preempt_skip" "SELECT count() FROM bs WHERE ${BIG_COND} SETTINGS ${CAP}"
# Two sort columns make the top-K selection of a part queue every granule (ties); LIMIT 10 needs the top-K limit >= 10
preempted_inside_part "${P}_preempt_top_k" "SELECT count() FROM (SELECT b FROM big ORDER BY b, a LIMIT 10) SETTINGS use_skip_indexes_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 1000, ${CAP}"

echo "--- no hang in a workload with one CPU slot"
ONE="workload = '${P}_one', max_threads = 8, ${COMMON}"
timeout 300 $CLICKHOUSE_CLIENT --query_id "${P}_oneslot_1" -q "SELECT count() FROM t WHERE ${T_COND} SETTINGS ${ONE}"
timeout 300 $CLICKHOUSE_CLIENT --query_id "${P}_oneslot_2" -q "SELECT count() FROM p WHERE v IN (SELECT number FROM numbers(10)) SETTINGS use_index_for_in_with_subqueries = 1, force_data_skipping_indices = 'iv', ${ONE}"
timeout 300 $CLICKHOUSE_CLIENT --query_id "${P}_oneslot_3" -q "SELECT count() FROM pr WHERE v = 7 SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'pv', ${ONE}"
timeout 300 $CLICKHOUSE_CLIENT --query_id "${P}_oneslot_4" -q "SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), t, ${T_COND}) SETTINGS ${ONE}"
timeout 300 $CLICKHOUSE_CLIENT --query_id "${P}_oneslot_5" -q "SELECT count() FROM numbers(1) WHERE dictHas('${P}_dict', number + 1) SETTINGS ${ONE}"

echo "--- KILL QUERY wakes up an analysis starved by a higher priority query"
# Without the limits of the CI profile, which stop it at once
$CLICKHOUSE_CLIENT --query_id "${P}_hog" -q "SELECT count() FROM numbers_mt(1e13) SETTINGS workload = '${P}_hi', max_threads = 1, use_concurrency_control = 1, max_rows_to_read = 0, max_bytes_to_read = 0, max_estimated_execution_time = 0" >/dev/null 2>&1 &
for _ in {1..300}; do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${P}_hog'")" = 1 ] && break
    sleep 0.1
done
timeout 300 $CLICKHOUSE_CLIENT --query_id "${P}_starved" -q "SELECT count() FROM t WHERE ${T_COND} SETTINGS workload = '${P}_lo', max_threads = 8, ${COMMON}" > "${CLICKHOUSE_TMP}/${P}_starved.out" 2>&1 &
starved_pid=$!
preempted=""
for _ in {1..600}; do
    preempted=$($CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['ConcurrencyControlPreemptions'] > 0 FROM system.processes WHERE query_id = '${P}_starved'")
    [ "$preempted" = 1 ] && break
    kill -0 $starved_pid 2>/dev/null || break
    sleep 0.1
done
echo "starved: ${preempted:-finished}"
[ "$preempted" = 1 ] && $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '${P}_starved' SYNC FORMAT Null"
wait $starved_pid
grep -c -m1 'QUERY_WAS_CANCELLED' "${CLICKHOUSE_TMP}/${P}_starved.out"

# replace_running_query only marks the query killed, the analysis notices it while it waits for a slot
timeout 300 $CLICKHOUSE_CLIENT --query_id "${P}_starved_r" -q "SELECT count() FROM t WHERE ${T_COND} SETTINGS workload = '${P}_lo', max_threads = 8, ${COMMON}" > "${CLICKHOUSE_TMP}/${P}_starved_r.out" 2>&1 &
starved_pid=$!
preempted=""
for _ in {1..600}; do
    preempted=$($CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['ConcurrencyControlPreemptions'] > 0 FROM system.processes WHERE query_id = '${P}_starved_r'")
    [ "$preempted" = 1 ] && break
    kill -0 $starved_pid 2>/dev/null || break
    sleep 0.1
done
echo "replaced: ${preempted:-finished}"
$CLICKHOUSE_CLIENT --query_id "${P}_starved_r" --replace_running_query 1 --replace_running_query_max_wait_ms 60000 -q "SELECT 1"
wait $starved_pid
grep -c -m1 'QUERY_WAS_CANCELLED' "${CLICKHOUSE_TMP}/${P}_starved_r.out"

echo "--- the client's cancellation (Ctrl-C) stops an analysis starved by a higher priority query"
$CLICKHOUSE_CLIENT --query_id "${P}_starved_c" -q "SELECT count() FROM t WHERE ${T_COND} SETTINGS workload = '${P}_lo', max_threads = 8, ${COMMON}" >/dev/null 2>&1 &
starved_pid=$!
preempted=""
for _ in {1..600}; do
    preempted=$($CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['ConcurrencyControlPreemptions'] > 0 FROM system.processes WHERE query_id = '${P}_starved_c'")
    [ "$preempted" = 1 ] && break
    kill -0 $starved_pid 2>/dev/null || break
    sleep 0.1
done
echo "cancelled: ${preempted:-finished}"
kill -INT $starved_pid
start=$SECONDS
while (( SECONDS - start < 120 )) && [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${P}_starved_c'")" != 0 ]; do
    sleep 0.1
done
# The analysis would otherwise wait until the hog stops
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '${P}_starved_c' SYNC FORMAT Null" 2>/dev/null
wait $starved_pid
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT errorCodeToName(exception_code) FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${P}_starved_c' AND type != 'QueryStart'"

echo "--- a soft time limit stops the wait of an analysis starved by a higher priority query"
timeout 120 $CLICKHOUSE_CLIENT --query_id "${P}_starved_b" -q "SELECT count() FROM t WHERE ${T_COND} SETTINGS workload = '${P}_lo', max_threads = 8, max_execution_time = 1, timeout_overflow_mode = 'break', ${COMMON}" >/dev/null; echo "break: $?"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '${P}_starved_b' SYNC FORMAT Null" 2>/dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['ConcurrencyControlPreemptions'] > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${P}_starved_b' AND type = 'QueryFinish'"

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '${P}_hog' SYNC FORMAT Null"
wait

echo "--- no hang in a workload with one CPU slot when the lease of the executor cannot be parked"
# Separate resources for MASTER THREAD and WORKER THREAD make every lease not parkable
$CLICKHOUSE_CLIENT -m -q "
    DROP RESOURCE ${RES};
    CREATE RESOURCE ${RES} (MASTER THREAD);
    CREATE RESOURCE ${RES}_w (WORKER THREAD);"
timeout 300 $CLICKHOUSE_CLIENT --query_id "${P}_oneslot_np" -q "SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), t, ${T_COND}) SETTINGS ${ONE}"
rm -f "${CLICKHOUSE_TMP}/${P}_starved.out" "${CLICKHOUSE_TMP}/${P}_starved_r.out"
