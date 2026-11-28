#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires Iceberg (USE_AVRO build option) and S3

# A table of a database with `lazy_load_tables` stays a proxy in the catalog for as long as a query holds the
# proxy, even after the table has been loaded. Here the target of a materialized view is an Iceberg table that
# is loaded and whose proxy is held by a reader, and new data is committed to the same Iceberg path, both through
# the view and by a table outside of the database. Reading the view refreshed the external metadata of the proxy,
# which does nothing, rather than of the loaded storage, so the view kept reading an old snapshot of the target.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
TARGET_PATH="05840_lazy_iceberg/${CLICKHOUSE_TEST_UNIQUE_NAME}/target"
READER="${CLICKHOUSE_DATABASE}_reader"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$READER' SYNC SETTINGS kill_throw_if_noop = 0 FORMAT Null" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS writer; DROP DATABASE IF EXISTS $DB SYNC;" 2>/dev/null
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.target (c0 String) ENGINE = IcebergS3(s3_conn, filename = '${TARGET_PATH}');
CREATE TABLE $DB.src (c0 String) ENGINE = Null;
CREATE MATERIALIZED VIEW $DB.mv TO $DB.target AS SELECT c0 FROM $DB.src;
CREATE TABLE writer ENGINE = IcebergS3(s3_conn, filename = '${TARGET_PATH}');
"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO $DB.src SELECT 'old' FROM numbers(100)"

# Reloading the database installs the proxies, as a server restart does.
$CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; ATTACH DATABASE $DB;"

# The reader loads the target through the proxy and holds the proxy until it is killed. The limit on the time of
# sleeping per block is lifted, so the reader does not end with `TOO_SLOW`, which would release the proxy.
$CLICKHOUSE_CLIENT --query_id "$READER" --function_sleep_max_microseconds_per_block 0 --query "
SELECT sleepEachRow(1) FROM $DB.target FORMAT Null" >/dev/null 2>&1 &

# Wait until the reader has loaded the target, i.e. the engine is known.
for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$READER'")" = 1 ] \
        && [ "$($CLICKHOUSE_CLIENT --query "SELECT engine FROM system.tables WHERE database = '$DB' AND name = 'target'")" = IcebergS3 ] \
        && break
    sleep 0.1
done

# An insert through the view refreshes the external metadata of the loaded target, which from then on reads
# the snapshot of that moment until the next refresh. The reader does not refresh it: it started earlier.
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO $DB.src VALUES ('through the view')"

# Commit a new snapshot behind the back of the lazy database.
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO writer VALUES ('from outside')"

# The reader still holds the proxy, so the view gets the proxy as its target and has to refresh the loaded storage.
$CLICKHOUSE_CLIENT --query "SELECT 'the reader is running', count() FROM system.processes WHERE query_id = '$READER'"
$CLICKHOUSE_CLIENT --query "SELECT c0, count() FROM $DB.mv GROUP BY c0 ORDER BY c0"

$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$READER' SYNC SETTINGS kill_throw_if_noop = 0 FORMAT Null"
wait
