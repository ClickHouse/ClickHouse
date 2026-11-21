#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-merge-tree-settings, no-parallel-replicas, no-replicated-database, no-shared-merge-tree
# no-fasttest, no-parallel: the failpoint and the primary index cache are global.
# no-random-merge-tree-settings: the test depends on granules and on how the primary index is loaded.
# no-parallel-replicas, no-replicated-database, no-shared-merge-tree: the test counts the loads of indexes by one query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT parts_splitter_pause_before_index"' EXIT

# 4 intersecting parts, the primary index of each takes 20 MB.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (k String, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 1;
    SYSTEM STOP MERGES t;
    INSERT INTO t SELECT leftPad(toString(number % 2000 * 4 + intDiv(number, 2000)), 5, '0') || repeat('a', 10000), number FROM numbers(8000)
    SETTINGS max_block_size = 2000, min_insert_block_size_rows = 2000, min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1;
"

# An index is accounted as memory usage of the query only if neither the part nor the cache keeps it.
# The arguments are the name, the initial setting, and what is done before each index is loaded.
function check()
{
    echo "$1"
    $CLICKHOUSE_CLIENT -q "
        ALTER TABLE t MODIFY SETTING use_primary_key_cache = $2;
        SYSTEM UNLOAD PRIMARY KEY t;
        SYSTEM DROP PRIMARY INDEX CACHE;
        SYSTEM ENABLE FAILPOINT parts_splitter_pause_before_index;
    "

    {
        $CLICKHOUSE_CLIENT -q "
            SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT sum(v) FROM t FINAL)
            SETTINGS max_memory_usage = '30M', max_threads = 1, split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, log_comment = '$1'
        " 2>&1 | grep -o -m1 -E '^1$|MEMORY_LIMIT_EXCEEDED'

        # Do not wait for a pause if the query is finished.
        $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT parts_splitter_pause_before_index"
    } &

    $CLICKHOUSE_CLIENT -q "
        SYSTEM WAIT FAILPOINT parts_splitter_pause_before_index PAUSE; $3; SYSTEM NOTIFY FAILPOINT parts_splitter_pause_before_index;
        SYSTEM WAIT FAILPOINT parts_splitter_pause_before_index PAUSE; $4; SYSTEM NOTIFY FAILPOINT parts_splitter_pause_before_index;
        SYSTEM WAIT FAILPOINT parts_splitter_pause_before_index PAUSE; $5; SYSTEM NOTIFY FAILPOINT parts_splitter_pause_before_index;
        SYSTEM WAIT FAILPOINT parts_splitter_pause_before_index PAUSE; $6; SYSTEM DISABLE FAILPOINT parts_splitter_pause_before_index;
    "
    wait
}

nothing="SELECT 1 FORMAT Null"
enable="ALTER TABLE t MODIFY SETTING use_primary_key_cache = 1"
disable="ALTER TABLE t MODIFY SETTING use_primary_key_cache = 0"
drop="SYSTEM DROP PRIMARY INDEX CACHE"
unload="SYSTEM UNLOAD PRIMARY KEY t"

check "enable" 0 "$enable" "$nothing" "$nothing" "$nothing"
check "enable and drop" 0 "$enable" "$nothing" "$nothing" "$drop"
check "disable first and drop" 1 "$disable" "$nothing" "$nothing" "$drop"
check "disable last" 1 "$nothing" "$nothing" "$nothing" "$disable"
check "unload last" 0 "$nothing" "$nothing" "$nothing" "$unload"
check "unload always" 0 "$unload" "$unload" "$unload" "$unload"

# The query is stopped before all indexes are loaded.
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['LoadedPrimaryIndexFiles'] FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'unload always' AND type != 'QueryStart' AND event_date >= yesterday();
    DROP TABLE t;
"
