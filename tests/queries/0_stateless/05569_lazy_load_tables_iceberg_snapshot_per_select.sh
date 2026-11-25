#!/usr/bin/env bash
# Tags: no-fasttest

# Two `SELECT`s of one query that pick different snapshots of the same data lake table, which is a lazy stand-in
# in a database with `lazy_load_tables`, must each read their own snapshot.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
DB_LAZY="${CLICKHOUSE_DATABASE}_lazy"
trap 'rm -rf "${DIR}"' EXIT

$CLICKHOUSE_CLIENT --allow_insert_into_iceberg 1 --allow_experimental_iceberg_compaction 0 --iceberg_delete_data_on_drop 0 -m -q "
DROP DATABASE IF EXISTS ${DB_LAZY};
CREATE DATABASE ${DB_LAZY} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE ${DB_LAZY}.t (id Int64) ENGINE = IcebergLocal('${DIR}', 'Parquet');
INSERT INTO ${DB_LAZY}.t VALUES (100);
INSERT INTO ${DB_LAZY}.t VALUES (7);
CREATE TABLE ${DB_LAZY}.tf (id Int64) AS icebergLocal('${DIR}', 'Parquet');
"

S1=$($CLICKHOUSE_CLIENT -q "SELECT snapshot_id FROM system.iceberg_history WHERE database = '${DB_LAZY}' AND table = 't' ORDER BY made_current_at LIMIT 1")
S2=$($CLICKHOUSE_CLIENT -q "SELECT snapshot_id FROM system.iceberg_history WHERE database = '${DB_LAZY}' AND table = 't' ORDER BY made_current_at DESC LIMIT 1")

$CLICKHOUSE_CLIENT -m -q "
DETACH DATABASE ${DB_LAZY};
ATTACH DATABASE ${DB_LAZY};
SELECT engine, is_loaded FROM system.tables WHERE database = '${DB_LAZY}' AND name = 't';

SELECT * FROM
(
    (SELECT 'old', sum(id) FROM ${DB_LAZY}.t SETTINGS iceberg_snapshot_id = ${S1})
    UNION ALL
    (SELECT 'new', sum(id) FROM ${DB_LAZY}.t SETTINGS iceberg_snapshot_id = ${S2})
)
ORDER BY 1;

SELECT * FROM
(
    (SELECT 'new', sum(id) FROM ${DB_LAZY}.t SETTINGS iceberg_snapshot_id = ${S2})
    UNION ALL
    (SELECT 'old', sum(id) FROM ${DB_LAZY}.t SETTINGS iceberg_snapshot_id = ${S1})
)
ORDER BY 1;

SELECT 'old', count() FROM ${DB_LAZY}.t SETTINGS iceberg_snapshot_id = ${S1};
SELECT 'new', count() FROM ${DB_LAZY}.t SETTINGS iceberg_snapshot_id = ${S2};

SELECT 'table function';
SELECT * FROM
(
    (SELECT 'old', sum(id) FROM ${DB_LAZY}.tf SETTINGS iceberg_snapshot_id = ${S1})
    UNION ALL
    (SELECT 'new', sum(id) FROM ${DB_LAZY}.tf SETTINGS iceberg_snapshot_id = ${S2})
)
ORDER BY 1;

SELECT * FROM
(
    (SELECT 'new', sum(id) FROM ${DB_LAZY}.tf SETTINGS iceberg_snapshot_id = ${S2})
    UNION ALL
    (SELECT 'old', sum(id) FROM ${DB_LAZY}.tf SETTINGS iceberg_snapshot_id = ${S1})
)
ORDER BY 1;
"

# The schema of the table also belongs to the snapshot: a column renamed between the snapshots must not leak
# into the `SELECT` of the older one through the shared table function proxy. The `SELECT`s of the newer snapshots
# count the files, because the proxy keeps the columns from the `CREATE` query and `count()` could read `id`,
# which those snapshots do not have, see https://github.com/ClickHouse/ClickHouse/issues/129062.
DIR2="${DIR}evolved/"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg 1 --allow_experimental_iceberg_compaction 0 --iceberg_delete_data_on_drop 0 -m -q "
CREATE TABLE ${DB_LAZY}.t2 (id Int64) ENGINE = IcebergLocal('${DIR2}', 'Parquet');
INSERT INTO ${DB_LAZY}.t2 VALUES (100);
CREATE TABLE ${DB_LAZY}.tf2 (id Int64) AS icebergLocal('${DIR2}', 'Parquet');
ALTER TABLE ${DB_LAZY}.t2 RENAME COLUMN id TO renamed;
"

# 16 more snapshots with the renamed column, so that one query pins many states of the table.
for i in {1..16}; do
    $CLICKHOUSE_CLIENT --allow_insert_into_iceberg 1 --allow_experimental_iceberg_compaction 0 -q "INSERT INTO ${DB_LAZY}.t2 VALUES (${i})"
done

mapfile -t SNAPSHOTS < <($CLICKHOUSE_CLIENT -q "SELECT snapshot_id FROM system.iceberg_history WHERE database = '${DB_LAZY}' AND table = 't2' ORDER BY made_current_at")
E1=${SNAPSHOTS[0]}
E2=${SNAPSHOTS[1]}

# The `SELECT` of the oldest snapshot first, then one `SELECT` for each of the other 16 snapshots.
MANY_STATES="(SELECT 'old', sum(id) FROM ${DB_LAZY}.tf2 SETTINGS iceberg_snapshot_id = ${E1})"
for i in {1..16}; do
    MANY_STATES+=" UNION ALL (SELECT 'new ${i}', toInt64(uniqExact(_file)) FROM ${DB_LAZY}.tf2 SETTINGS iceberg_snapshot_id = ${SNAPSHOTS[$i]})"
done

$CLICKHOUSE_CLIENT -m -q "
DETACH DATABASE ${DB_LAZY};
ATTACH DATABASE ${DB_LAZY};

SELECT 'schema evolution';
SELECT * FROM
(
    (SELECT 'old', sum(id) FROM ${DB_LAZY}.tf2 SETTINGS iceberg_snapshot_id = ${E1})
    UNION ALL
    (SELECT 'new', toInt64(uniqExact(_file)) FROM ${DB_LAZY}.tf2 SETTINGS iceberg_snapshot_id = ${E2})
)
ORDER BY 1;

SELECT * FROM
(
    (SELECT 'new', toInt64(uniqExact(_file)) FROM ${DB_LAZY}.tf2 SETTINGS iceberg_snapshot_id = ${E2})
    UNION ALL
    (SELECT 'old', sum(id) FROM ${DB_LAZY}.tf2 SETTINGS iceberg_snapshot_id = ${E1})
)
ORDER BY 1;

SELECT 'many states';
SELECT * FROM (${MANY_STATES}) ORDER BY 2;

DROP DATABASE ${DB_LAZY};
"
