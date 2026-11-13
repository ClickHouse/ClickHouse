#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `reattach_tables_before_query_execution_probability` must skip the tables used by another query.
# In a database with `lazy_load_tables`, the queries lock the storage behind the stand-in, not the stand-in itself.
# If the table was reattached under a running `INSERT`, the inserted data would be written into the old storage.

db="${CLICKHOUSE_DATABASE}_lazy"
query_id="05628_insert_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS $db;
    CREATE DATABASE $db ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE $db.mt (x UInt64) ENGINE = MergeTree ORDER BY x;
    DETACH DATABASE $db;
    ATTACH DATABASE $db;
    INSERT INTO $db.mt VALUES (0);
"

$CLICKHOUSE_CLIENT --query_id "$query_id" --max_block_size 1 --async_insert 0 -q "
    INSERT INTO $db.mt SELECT number + 1 FROM numbers(3) WHERE NOT sleepEachRow(1)
" &
insert_pid=$!

# Wait until the `INSERT` is running, so that it holds its lock on the table.
while kill -0 "$insert_pid" 2>/dev/null && [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id' AND read_rows > 0")" = 0 ]
do
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "USE $db; SELECT 1 FORMAT Null SETTINGS reattach_tables_before_query_execution_probability = 1"

wait

$CLICKHOUSE_CLIENT -q "
    SELECT count(), sum(x) FROM $db.mt;
    DROP DATABASE $db;
"
