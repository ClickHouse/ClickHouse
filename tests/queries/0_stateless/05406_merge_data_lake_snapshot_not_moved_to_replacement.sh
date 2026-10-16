#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-msan
# Tag no-parallel: this test WAITS on a process-global PAUSEABLE failpoint.
# Tag no-msan: delta-kernel-rs is not built with MSan.

# `merge()` reuses the snapshot it inferred its columns from only for the same table: when another table takes the
# name of a source table without a UUID in between, the read must use the snapshot of that table, not a version
# number of the replaced one.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP="delta_lake_metadata_iterate_pause"
DB="${CLICKHOUSE_DATABASE}_ordinary"
DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
SETTINGS="--allow_experimental_delta_kernel_rs 1 --allow_delta_lake_writes 1 --allow_delta_lake_create_table 1"
trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}" ||:; $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB}" ||:; rm -rf "${DIR}"' EXIT

# The first `Ordinary` database of the server logs a deprecation warning.
$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary 1 --send_logs_level fatal -q "CREATE DATABASE ${DB} ENGINE = Ordinary"
$CLICKHOUSE_CLIENT $SETTINGS -m -q "
CREATE TABLE ${DB}.t (k Int64) ENGINE = DeltaLakeLocal('${DIR}/t/', Parquet);
INSERT INTO ${DB}.t VALUES (7);
CREATE TABLE ${DB}.replacement (k Int64) ENGINE = DeltaLakeLocal('${DIR}/replacement/', Parquet);
INSERT INTO ${DB}.replacement VALUES (100);
INSERT INTO ${DB}.replacement VALUES (200);
CREATE TABLE ${DB}.pause (k Int64) ENGINE = DeltaLakeLocal('${DIR}/pause/', Parquet);
INSERT INTO ${DB}.pause VALUES (1);
SYSTEM ENABLE FAILPOINT ${FP};
"

# The scalar subquery pauses after `merge()` inferred its columns and before its read looks up the tables again.
$CLICKHOUSE_CLIENT $SETTINGS -q "SELECT sum(k) FROM merge('${DB}', '^t\$') WHERE (SELECT max(k) FROM ${DB}.pause) = 1" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${DB}.t TO ${DB}.old, ${DB}.replacement TO ${DB}.t"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}"
wait
