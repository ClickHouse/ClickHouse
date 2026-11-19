#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-parallel: uses a server-wide failpoint.
# Rename-first ordering: a DROP DATABASE that starts while RENAME TABLE of a TimeSeries table is between two
# inner-table renames must wait for the whole RENAME and must not interrupt it halfway.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP=time_series_rename_pause_after_first_inner_table
SRC="${CLICKHOUSE_DATABASE}_src"
DST="${CLICKHOUSE_DATABASE}_dst"
SETTINGS="--allow_deprecated_database_ordinary=1 --allow_experimental_time_series_table=1"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
    $CLICKHOUSE_CLIENT $SETTINGS -q "DROP DATABASE IF EXISTS $SRC" 2>/dev/null
    $CLICKHOUSE_CLIENT $SETTINGS -q "DROP DATABASE IF EXISTS $DST" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT $SETTINGS -q "
    CREATE DATABASE $SRC ENGINE = Ordinary;
    CREATE DATABASE $DST ENGINE = Ordinary;
    CREATE TABLE $SRC.ts ENGINE = TimeSeries;
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# 1. RENAME starts, renames the first inner table and pauses (it holds the database locks).
$CLICKHOUSE_CLIENT $SETTINGS --query_id "${CLICKHOUSE_DATABASE}_rename" -q "RENAME TABLE $SRC.ts TO $DST.ts" >/dev/null 2>&1 &
rename_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"

# 2. DROP DATABASE starts while the RENAME is paused and has to wait for it.
$CLICKHOUSE_CLIENT $SETTINGS --query_id "${CLICKHOUSE_DATABASE}_drop" -q "DROP DATABASE $DST" >/dev/null 2>&1 &
drop_pid=$!
for _ in {1..100}
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${CLICKHOUSE_DATABASE}_drop'")" = "1" ] && break
    sleep 0.1
done
sleep 1

# 3. Let the RENAME continue with the next inner tables.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"

wait $rename_pid && echo "rename ok" || echo "rename failed"
wait $drop_pid

# With the fix the RENAME finishes first, so nothing is left in the source database.
# Before it, the nested rename failed, the outer table was restored in `src` and a table was stranded in `dst`.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '$SRC'"
