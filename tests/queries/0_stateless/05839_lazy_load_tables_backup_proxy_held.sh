#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table of a database with `lazy_load_tables` stays a proxy in the catalog for as long as a query holds the
# proxy, even after the table has been loaded. `BACKUP` then reaches the proxy on every pass over the tables,
# and the proxy did not forward `backupData`: the backup succeeded with the table definition and no data, and
# its restore silently gave an empty table.
#
# The proxy does not forward `totalBytesUncompressed`, so `system.tables.total_bytes_uncompressed`
# tells the proxy (NULL) from the loaded `MergeTree` (not NULL).

DB="${CLICKHOUSE_DATABASE}_lazy"
BACKUP_NAME="${CLICKHOUSE_DATABASE}_held"
READER="${CLICKHOUSE_DATABASE}_reader"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$READER' SYNC SETTINGS kill_throw_if_noop = 0 FORMAT Null" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB SYNC" 2>/dev/null
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.mt (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO $DB.mt SELECT number FROM numbers(100);
DETACH DATABASE $DB;
ATTACH DATABASE $DB;
"

# The reader loads the table through the proxy and holds the proxy until it is killed. The limit on the time of
# sleeping per block is lifted, so the reader does not end with `TOO_SLOW`, which would release the proxy.
$CLICKHOUSE_CLIENT --query_id "$READER" --function_sleep_max_microseconds_per_block 0 --query "
SELECT sleepEachRow(1) FROM $DB.mt FORMAT Null" >/dev/null 2>&1 &

# Wait until the reader has loaded the table, i.e. the engine is known.
for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$READER'")" = 1 ] \
        && [ "$($CLICKHOUSE_CLIENT --query "SELECT engine FROM system.tables WHERE database = '$DB' AND name = 'mt'")" = MergeTree ] \
        && break
    sleep 0.1
done

# The reader holds the proxy, so it stays in the catalog over the whole backup. A backup to `Memory` lives only
# as long as the session, so everything is done in one.
IS_PROXY="SELECT total_bytes_uncompressed IS NULL FROM system.tables WHERE database = '$DB' AND name = 'mt'"
$CLICKHOUSE_CLIENT --query "
SELECT 'proxy before the backup', ($IS_PROXY);
BACKUP TABLE $DB.mt TO Memory('$BACKUP_NAME') FORMAT Null;
SELECT 'proxy after the backup', ($IS_PROXY);
RESTORE TABLE $DB.mt AS $DB.restored FROM Memory('$BACKUP_NAME') FORMAT Null;
SELECT 'restored into another table', count(), sum(a) FROM $DB.restored;
"

$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$READER' SYNC SETTINGS kill_throw_if_noop = 0 FORMAT Null"
wait
