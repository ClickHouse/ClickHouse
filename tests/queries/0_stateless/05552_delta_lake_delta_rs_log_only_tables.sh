#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: `deltaLakeLocal` needs the delta-kernel-rs library, which fast test does not build.
# no-msan: delta-kernel-rs is Rust code that is not instrumented by MSan.

# Delta log replay on tables adopted from the delta-rs test suite (see data_delta_lake/delta_rs/README.md)
# that ship only `_delta_log` without data files. `count()` is answered from the `numRecords` statistics
# in the log, so these queries exercise log replay, checkpoints and time travel without reading data.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake/delta_rs"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT '-- checkpoints: 12 JSON commits, one row each, no checkpoint';
DESCRIBE deltaLakeLocal('$DATA/checkpoints');
SELECT count() FROM deltaLakeLocal('$DATA/checkpoints');
SELECT count() FROM deltaLakeLocal('$DATA/checkpoints') SETTINGS delta_lake_snapshot_version = 0;
SELECT count() FROM deltaLakeLocal('$DATA/checkpoints') SETTINGS delta_lake_snapshot_version = 1;
SELECT count() FROM deltaLakeLocal('$DATA/checkpoints') SETTINGS delta_lake_snapshot_version = 5;
SELECT count() FROM deltaLakeLocal('$DATA/checkpoints') SETTINGS delta_lake_snapshot_version = 11;

SELECT '-- checkpoints_vacuumed: commits before the checkpoint at version 5 are removed';
SELECT count() FROM deltaLakeLocal('$DATA/checkpoints_vacuumed');
SELECT count() FROM deltaLakeLocal('$DATA/checkpoints_vacuumed') SETTINGS delta_lake_snapshot_version = 5;
SELECT count() FROM deltaLakeLocal('$DATA/checkpoints_vacuumed') SETTINGS delta_lake_snapshot_version = 7;
SELECT count() FROM deltaLakeLocal('$DATA/checkpoints_vacuumed') SETTINGS delta_lake_snapshot_version = 10;

SELECT '-- latest_not_checkpointed: the newest commit is after the checkpoint at version 2';
DESCRIBE deltaLakeLocal('$DATA/latest_not_checkpointed');
SELECT count() FROM deltaLakeLocal('$DATA/latest_not_checkpointed');
SELECT count() FROM deltaLakeLocal('$DATA/latest_not_checkpointed') SETTINGS delta_lake_snapshot_version = 0;
SELECT count() FROM deltaLakeLocal('$DATA/latest_not_checkpointed') SETTINGS delta_lake_snapshot_version = 1;
SELECT count() FROM deltaLakeLocal('$DATA/latest_not_checkpointed') SETTINGS delta_lake_snapshot_version = 2;

SELECT '-- table-with-domain-metadata: only a checkpoint at version 108 with domain metadata';
DESCRIBE deltaLakeLocal('$DATA/table-with-domain-metadata');
SELECT count() FROM deltaLakeLocal('$DATA/table-with-domain-metadata');
"

echo "-- a version whose commits were vacuumed cannot be read"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM deltaLakeLocal('$DATA/checkpoints_vacuumed') SETTINGS delta_lake_snapshot_version = 4" 2>&1 | grep -o -m1 'DELTA_KERNEL_ERROR'
$CLICKHOUSE_LOCAL -q "SELECT count() FROM deltaLakeLocal('$DATA/table-with-domain-metadata') SETTINGS delta_lake_snapshot_version = 107" 2>&1 | grep -o -m1 'DELTA_KERNEL_ERROR'
