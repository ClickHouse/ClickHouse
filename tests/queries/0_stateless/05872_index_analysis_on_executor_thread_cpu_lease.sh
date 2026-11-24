#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: the server allows only one MASTER THREAD / WORKER THREAD resource at a time
# no-fasttest: inserts into a table with index_granularity = 1

# `mergeTreeAnalyzeIndexes` analyzes indexes on a thread of the pipeline executor, which already runs on a CPU lease.
# The analysis must not wait for one more CPU slot: in a workload with one slot it is the slot that this thread holds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKLOAD="${CLICKHOUSE_DATABASE}_one_slot"
# Not per database: `CREATE OR REPLACE` then also replaces the resource of a killed run
RESOURCE="test_05872_cpu"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id LIKE '${CLICKHOUSE_DATABASE}_analysis_%' FORMAT Null" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS ${WORKLOAD}" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS ${RESOURCE}" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS ${RESOURCE}_worker" 2>/dev/null
}
trap cleanup EXIT

# With index_granularity = 1 and a filter on the second column of the key, the generic exclusion search of a part
# takes more CPU than the quantum of a CPU slot, so the lease of the analysis is renewed.
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b)
        SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';
    SYSTEM STOP MERGES t;
    INSERT INTO t SELECT number % 25000, number FROM numbers(250000);
    INSERT INTO t SELECT number % 25000, number FROM numbers(250000);
    INSERT INTO t SELECT number % 25000, number FROM numbers(250000);
    INSERT INTO t SELECT number % 25000, number FROM numbers(250000);

    DROP RESOURCE IF EXISTS ${RESOURCE}_worker;
    CREATE OR REPLACE RESOURCE ${RESOURCE} (MASTER THREAD, WORKER THREAD);
    CREATE WORKLOAD ${WORKLOAD} SETTINGS max_concurrent_threads = 1;
"

analyze()
{
    timeout 60 $CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_DATABASE}_analysis_$1" -q "
        SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), t,
            b IN (1, 15626, 31251, 46876, 62501, 78126, 93751, 109376, 125001, 140626, 156251, 171876, 187501, 203126, 218751, 234376))
        SETTINGS workload = '${WORKLOAD}', max_threads = 8, max_threads_for_indexes = 0, use_concurrency_control = 1,
            use_query_condition_cache = 0, merge_tree_generic_exclusion_search_max_steps = 0" \
        || echo "The analysis has not finished"
}

echo "A lease that can be parked"
analyze parkable

# Separate resources for the master thread and for the worker threads make the leases not parkable.
$CLICKHOUSE_CLIENT -m -q "
    DROP WORKLOAD ${WORKLOAD};
    DROP RESOURCE ${RESOURCE};
    CREATE RESOURCE ${RESOURCE} (MASTER THREAD);
    CREATE RESOURCE ${RESOURCE}_worker (WORKER THREAD);
    CREATE WORKLOAD ${WORKLOAD} SETTINGS max_concurrent_threads = 1;
"

echo "A lease that cannot be parked"
analyze not_parkable
