#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: this test WAITS on a process-global PAUSEABLE failpoint.

# `merge()` infers its columns from a snapshot of the data lake table, and its read must use the same
# snapshot: a column that another writer renames in between must not be read as missing (defaults).
# The snapshot is reused only for the same table: a table without a UUID that takes the name of the source
# table in between must be read at its own snapshot.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP="storage_merge_create_children_plans_pause"
DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB="${CLICKHOUSE_DATABASE}_ordinary"
SETTINGS="--allow_insert_into_iceberg 1 --allow_experimental_iceberg_compaction 0 --iceberg_delete_data_on_drop 0"
trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}" ||:; $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB}" ||:; rm -rf "${DIR}"' EXIT

# The first `Ordinary` database of the server logs a deprecation warning.
$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary 1 --send_logs_level fatal -q "CREATE DATABASE ${DB} ENGINE = Ordinary"
$CLICKHOUSE_CLIENT $SETTINGS -m -q "
CREATE TABLE t_lake (k Int64) ENGINE = IcebergLocal('${DIR}/t_lake/', 'Parquet');
INSERT INTO t_lake VALUES (7);
CREATE TABLE t_writer ENGINE = IcebergLocal('${DIR}/t_lake/', 'Parquet');
CREATE TABLE ${DB}.t (k Int64) ENGINE = IcebergLocal('${DIR}/t/', 'Parquet');
INSERT INTO ${DB}.t VALUES (7);
CREATE TABLE ${DB}.replacement (k Int64) ENGINE = IcebergLocal('${DIR}/replacement/', 'Parquet');
INSERT INTO ${DB}.replacement VALUES (100);
INSERT INTO ${DB}.replacement VALUES (200);
CREATE TABLE ${DB}.pause (k Int64) ENGINE = Memory;
INSERT INTO ${DB}.pause VALUES (1);
SYSTEM ENABLE FAILPOINT ${FP};
"

$CLICKHOUSE_CLIENT -q "SELECT sum(k) FROM merge(currentDatabase(), '^t_lake\$')" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
$CLICKHOUSE_CLIENT $SETTINGS -q "ALTER TABLE t_writer RENAME COLUMN k TO v"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}"
wait

# The scalar subquery pauses after `merge()` inferred its columns and before its read looks up the tables again.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT -q "SELECT sum(k) FROM merge('${DB}', '^t\$') WHERE (SELECT max(k) FROM merge('${DB}', '^pause\$')) = 1" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${DB}.t TO ${DB}.old, ${DB}.replacement TO ${DB}.t"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}"
wait

$CLICKHOUSE_CLIENT $SETTINGS -m -q "DROP TABLE t_writer; DROP TABLE t_lake;"
