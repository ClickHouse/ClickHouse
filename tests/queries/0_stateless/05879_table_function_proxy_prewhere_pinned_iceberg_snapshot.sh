#!/usr/bin/env bash
# Tags: no-fasttest

# A table created as a table function keeps the columns from the `CREATE` query, reads the nested storage and
# converts the types after the read, so PREWHERE and a row policy are pushed down only for the columns whose
# type is the same in both. The schema of an Iceberg table belongs to the snapshot, and two `SELECT`s of one query
# can pin different snapshots, so that decision has to be made for the snapshot that the `SELECT` reads, not for
# the one that was analyzed last.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
trap 'rm -rf "${DIR}"' EXIT

ICEBERG_SETTINGS="--allow_insert_into_iceberg 1 --allow_experimental_iceberg_compaction 0 --iceberg_delete_data_on_drop 0"

# `id` is `Int64` in the first snapshot and `Nullable(Int64)` in the second one, as in the proxy.
$CLICKHOUSE_CLIENT $ICEBERG_SETTINGS -m -q "
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS tf;
CREATE TABLE t (id Int64, value Int64) ENGINE = IcebergLocal('${DIR}', 'Parquet');
INSERT INTO t VALUES (0, 10), (1, 11);
ALTER TABLE t MODIFY COLUMN id Nullable(Int64);
INSERT INTO t VALUES (2, 12), (NULL, 13);
CREATE TABLE tf (id Nullable(Int64), value Int64) AS icebergLocal('${DIR}', 'Parquet');
"

S1=$($CLICKHOUSE_CLIENT -q "SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = 't' ORDER BY made_current_at LIMIT 1")
S2=$($CLICKHOUSE_CLIENT -q "SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = 't' ORDER BY made_current_at DESC LIMIT 1")

OLD="SELECT 'old', id, value FROM tf WHERE id != 0 SETTINGS iceberg_snapshot_id = ${S1}"
NEW="SELECT 'new', id, value FROM tf WHERE id != 0 SETTINGS iceberg_snapshot_id = ${S2}"
OLD_PREWHERE="SELECT 'old', id, value FROM tf PREWHERE id != 0 SETTINGS iceberg_snapshot_id = ${S1}"
NEW_PREWHERE="SELECT 'new', id, value FROM tf PREWHERE id != 0 SETTINGS iceberg_snapshot_id = ${S2}"
OLD_PREWHERE_VALUE="SELECT 'old', id, value FROM tf PREWHERE value != 10 SETTINGS iceberg_snapshot_id = ${S1}"

function run()
{
    local result
    result=$($CLICKHOUSE_CLIENT -q "$1" 2>&1)
    if grep -q -F 'ILLEGAL_PREWHERE' <<< "$result"; then
        echo "ILLEGAL_PREWHERE"
    else
        echo "$result"
    fi
}

echo "WHERE"
run "SELECT * FROM (($OLD) UNION ALL ($NEW)) ORDER BY ALL"
run "SELECT * FROM (($NEW) UNION ALL ($OLD)) ORDER BY ALL"

echo "PREWHERE on the column whose type differs in the old snapshot"
run "$OLD_PREWHERE"
run "SELECT * FROM (($OLD_PREWHERE) UNION ALL ($NEW)) ORDER BY ALL"
run "SELECT * FROM (($NEW) UNION ALL ($OLD_PREWHERE)) ORDER BY ALL"

echo "PREWHERE on the same column in the new snapshot"
run "SELECT * FROM ($NEW_PREWHERE) ORDER BY ALL"
run "SELECT * FROM (($OLD) UNION ALL ($NEW_PREWHERE)) ORDER BY ALL"
run "SELECT * FROM (($NEW_PREWHERE) UNION ALL ($OLD)) ORDER BY ALL"

echo "PREWHERE on a column whose type is the same"
run "SELECT * FROM (($OLD_PREWHERE_VALUE) UNION ALL ($NEW)) ORDER BY ALL"
run "SELECT * FROM (($NEW) UNION ALL ($OLD_PREWHERE_VALUE)) ORDER BY ALL"

echo "row policy"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY ${CLICKHOUSE_TEST_UNIQUE_NAME} ON tf FOR SELECT USING id != 0 TO CURRENT_USER"
OLD_ALL="SELECT 'old', id, value FROM tf SETTINGS iceberg_snapshot_id = ${S1}"
NEW_ALL="SELECT 'new', id, value FROM tf SETTINGS iceberg_snapshot_id = ${S2}"
run "SELECT * FROM (($OLD_ALL) UNION ALL ($NEW_ALL)) ORDER BY ALL"
run "SELECT * FROM (($NEW_ALL) UNION ALL ($OLD_ALL)) ORDER BY ALL"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY ${CLICKHOUSE_TEST_UNIQUE_NAME} ON tf"

$CLICKHOUSE_CLIENT -q "DROP TABLE tf"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"
