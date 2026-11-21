#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# A global failpoint pauses primary-key exclusion searches across the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

failpoint=pause_before_primary_key_exclusion_search
query_id="pk_exclusion_cancel_$(random_str 10)"
client_error=$(mktemp "$CLICKHOUSE_TMP/pk_exclusion_cancel.XXXXXX")
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint" >/dev/null 2>&1 || true; rm -f "$client_error"' EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pk_exclusion_cancel (k UInt64, v UInt64)
    ENGINE = MergeTree ORDER BY (k, v)
    SETTINGS index_granularity = 8, index_granularity_bytes = 0;
    SYSTEM STOP MERGES t_pk_exclusion_cancel;
    INSERT INTO t_pk_exclusion_cancel SETTINGS async_insert = 0
        SELECT number % 16, intDiv(number, 16) FROM numbers(1024);
    INSERT INTO t_pk_exclusion_cancel SETTINGS async_insert = 0
        SELECT number % 16, intDiv(number, 16) FROM numbers(1024);
"

# A disjunction on the second key column uses generic exclusion on each part.
query="
    SELECT count() FROM t_pk_exclusion_cancel
    WHERE v = 3 OR v = 7
    SETTINGS max_threads = 2, max_threads_for_indexes = 2,
        use_query_condition_cache = 0, use_statistics_for_part_pruning = 0,
        enable_parallel_replicas = 0,
        log_queries = 1, log_queries_probability = 1, log_profile_events = 1
"

$CLICKHOUSE_CLIENT --query_id "${query_id}_baseline" --query "$query"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $failpoint"

timeout 60 $CLICKHOUSE_CLIENT --query_id "$query_id" --query "$query" > /dev/null 2>"$client_error" &
client_pid=$!
timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $failpoint PAUSE"
timeout 30 $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$query_id' ASYNC FORMAT Null"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint"

if wait "$client_pid"; then
    echo "Query unexpectedly completed"
    exit 1
fi
grep -o -m1 'QUERY_WAS_CANCELLED' "$client_error"

# A later cancellation check would report the same exception after finishing the search.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT exception_code, ProfileEvents['IndexGenericExclusionSearchAlgorithm']
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND query_id IN ('$query_id', '${query_id}_baseline') AND type != 'QueryStart'
    ORDER BY exception_code;
"
