#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: this test WAITS on a process-global PAUSEABLE failpoint.

# `merge()` infers its columns from a snapshot of the data lake table, and its read must use the same
# snapshot: a column that another writer renames in between must not be read as missing (defaults).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP="storage_merge_create_children_plans_pause"
DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
SETTINGS="--allow_insert_into_iceberg 1 --allow_experimental_iceberg_compaction 0 --iceberg_delete_data_on_drop 0"
trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}" ||:; rm -rf "${DIR}"' EXIT

$CLICKHOUSE_CLIENT $SETTINGS -m -q "
CREATE TABLE t_lake (k Int64) ENGINE = IcebergLocal('${DIR}', 'Parquet');
INSERT INTO t_lake VALUES (7);
CREATE TABLE t_writer ENGINE = IcebergLocal('${DIR}', 'Parquet');
SYSTEM ENABLE FAILPOINT ${FP};
"

$CLICKHOUSE_CLIENT -q "SELECT sum(k) FROM merge(currentDatabase(), '^t_lake\$')" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
$CLICKHOUSE_CLIENT $SETTINGS -q "ALTER TABLE t_writer RENAME COLUMN k TO v"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}"
wait

$CLICKHOUSE_CLIENT $SETTINGS -m -q "DROP TABLE t_writer; DROP TABLE t_lake;"
