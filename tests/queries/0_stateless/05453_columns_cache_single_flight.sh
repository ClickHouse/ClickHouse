#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-random-settings, no-random-merge-tree-settings
# no-parallel: SYSTEM ENABLE FAILPOINT is process-wide, and SYSTEM DROP COLUMNS CACHE clears the
# cache of every other test.
# no-fasttest: a test that arms a fail point runs alone, and such tests are kept out of the fast test.
# no-random-settings: the part has to be read in blocks of one whole stripe.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Concurrent reads of the same stripes that are not in the columns cache read them from the part once:
# the first read claims the stripes it is about to read and write, the others wait for its write and
# are served from the cache - but no longer than reading the stripes themselves would take.

PAUSE=columns_cache_reader_pause_after_claim
UNBOUNDED=columns_cache_reader_unbounded_fill_wait

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $PAUSE" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $UNBOUNDED" 2>/dev/null
}
trap cleanup EXIT

# One stripe: 8 granules of 8192 rows.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_cc_flight;
    CREATE TABLE t_cc_flight (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;
    INSERT INTO t_cc_flight SELECT number, number * 3, number % 7 FROM numbers(65536);
"

QUERY="SELECT sum(a), sum(b) FROM t_cc_flight
    SETTINGS use_columns_cache = 1, max_threads = 1, use_query_condition_cache = 0, use_uncompressed_cache = 0"

# The first read stops right after it claimed the stripe; the second one is started then.
function start_claimant()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DROP COLUMNS CACHE"
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $PAUSE"
    $CLICKHOUSE_CLIENT --query_id "$1" --query "$QUERY" &
    claimant_pid=$!
    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $PAUSE PAUSE"
}

function finish_claimant()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $PAUSE"
    wait $claimant_pid
}

function wait_until_waiting()
{
    for _ in {1..600}; do
        waits=$($CLICKHOUSE_CLIENT --query "SELECT ProfileEvents['ColumnsCacheFillWaits'] FROM system.processes WHERE query_id = '$1'")
        [ "$waits" = "1" ] && return
        sleep 0.1
    done
    echo "query $1 did not wait"
}

function events()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    echo "waits, timeouts, misses"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['ColumnsCacheFillWaits'], ProfileEvents['ColumnsCacheFillWaitTimeouts'], ProfileEvents['ColumnsCacheMisses'] > 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$1' AND type = 'QueryFinish'"
}

# The second read runs to its end while the first one holds the stripe.
function read_while_claimed()
{
    start_claimant "$1"
    $CLICKHOUSE_CLIENT --query_id "$2" --query "$QUERY$3"
    finish_claimant
    events "$2"
}

ID="$CLICKHOUSE_DATABASE"

echo "-- the second read waits for the first one and is served from the cache"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $UNBOUNDED"
start_claimant "${ID}_1"
$CLICKHOUSE_CLIENT --query_id "${ID}_2" --query "$QUERY" &
waiter_pid=$!
wait_until_waiting "${ID}_2"
finish_claimant
wait $waiter_pid
events "${ID}_2"

echo "-- a waiting read can be cancelled"
start_claimant "${ID}_3"
$CLICKHOUSE_CLIENT --query_id "${ID}_4" --query "$QUERY" >/dev/null 2>&1 &
waiter_pid=$!
wait_until_waiting "${ID}_4"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${ID}_4' SYNC FORMAT Null"
wait $waiter_pid
finish_claimant
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT exception_code FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${ID}_4' AND type = 'ExceptionWhileProcessing'"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $UNBOUNDED"

echo "-- the wait is bounded: the second read finishes while the first one still holds the stripe"
read_while_claimed "${ID}_5" "${ID}_6"

echo "-- without columns_cache_wait_for_concurrent_fills the second read does not wait"
read_while_claimed "${ID}_7" "${ID}_8" ", columns_cache_wait_for_concurrent_fills = 0"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_cc_flight"
