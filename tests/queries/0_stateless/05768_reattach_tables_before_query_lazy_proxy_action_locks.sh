#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `reattach_tables_before_query_execution_probability` must keep the action locks of a lazily loaded table
# while another query still holds its `StorageTableProxy`. Then the database cannot replace the proxy with
# the loaded storage, but `ActionLocksManager` keeps the lock under the loaded storage.
# Here a slow scan of `system.tables` holds the proxy in its snapshot of the tables.

db="${CLICKHOUSE_DATABASE}_lazy"
query_id="05768_observer_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS $db;
    CREATE DATABASE $db ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE $db.mt (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $db.t1 (x UInt64) ENGINE = Memory;
    CREATE TABLE $db.t2 (x UInt64) ENGINE = Memory;
    CREATE TABLE $db.t3 (x UInt64) ENGINE = Memory;
    CREATE TABLE $db.t4 (x UInt64) ENGINE = Memory;
    CREATE TABLE $db.t5 (x UInt64) ENGINE = Memory;
    DETACH DATABASE $db;
    ATTACH DATABASE $db;
"

# With a column other than `name`, the scan keeps its snapshot of the tables across blocks.
# Takes 18 seconds unless killed.
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT name, engine, sleepEachRow(3) FROM system.tables WHERE database = '$db' FORMAT Null
    SETTINGS max_block_size = 1, max_threads = 1
" 2>/dev/null &
observer_pid=$!

# Wait until the scan has taken its snapshot of the tables, while `mt` is not loaded yet.
while kill -0 "$observer_pid" 2>/dev/null && [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id' AND read_rows > 0")" = 0 ]
do
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    SYSTEM STOP MERGES $db.mt;
    INSERT INTO $db.mt SELECT 1;
    INSERT INTO $db.mt SELECT 2;
    USE $db;
    SELECT 1 FORMAT Null SETTINGS reattach_tables_before_query_execution_probability = 1;
"

$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE $db.mt FINAL SETTINGS optimize_throw_if_noop = 1" 2>&1 | grep -o -m1 'ABORTED'
$CLICKHOUSE_CLIENT -q "SELECT 'active parts', count() FROM system.parts WHERE database = '$db' AND table = 'mt' AND active"

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null" 2>/dev/null
wait

$CLICKHOUSE_CLIENT -q "DROP DATABASE $db"
