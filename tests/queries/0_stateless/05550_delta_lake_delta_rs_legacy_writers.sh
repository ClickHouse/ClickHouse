#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: `deltaLakeLocal` needs the delta-kernel-rs library, which fast test does not build.
# no-msan: delta-kernel-rs is Rust code that is not instrumented by MSan.

# Delta tables written by old Spark/Delta and delta-rs releases, adopted from the delta-rs test suite
# (see data_delta_lake/delta_rs/README.md). The expected rows were checked against an independent
# replay of the Delta log over the raw Parquet files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake/delta_rs"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT '-- delta-0.2.0: two appends, an overwrite and an append, checkpoint at version 3';
SELECT * FROM deltaLakeLocal('$DATA/delta-0.2.0') ORDER BY ALL;
SELECT 'version 0', groupArray(value) FROM (SELECT value FROM deltaLakeLocal('$DATA/delta-0.2.0') ORDER BY value) SETTINGS delta_lake_snapshot_version = 0;
SELECT 'version 1', groupArray(value) FROM (SELECT value FROM deltaLakeLocal('$DATA/delta-0.2.0') ORDER BY value) SETTINGS delta_lake_snapshot_version = 1;
SELECT 'version 2', groupArray(value) FROM (SELECT value FROM deltaLakeLocal('$DATA/delta-0.2.0') ORDER BY value) SETTINGS delta_lake_snapshot_version = 2;

SELECT '-- delta-0.8.0: a write followed by a delete';
SELECT * FROM deltaLakeLocal('$DATA/delta-0.8.0') ORDER BY ALL;

SELECT '-- delta-0.8-empty: every row deleted';
SELECT count() FROM deltaLakeLocal('$DATA/delta-0.8-empty');
DESCRIBE deltaLakeLocal('$DATA/delta-0.8-empty');

SELECT '-- delta-0.8.0-date';
SELECT * FROM deltaLakeLocal('$DATA/delta-0.8.0-date') ORDER BY ALL;
DESCRIBE deltaLakeLocal('$DATA/delta-0.8.0-date');

SELECT '-- delta-0.8.0-partitioned: three partition columns';
SELECT * FROM deltaLakeLocal('$DATA/delta-0.8.0-partitioned') ORDER BY ALL;
SELECT value, day FROM deltaLakeLocal('$DATA/delta-0.8.0-partitioned') WHERE year = '2021' AND month = '12' ORDER BY ALL;
SELECT count() FROM deltaLakeLocal('$DATA/delta-0.8.0-partitioned') WHERE day = '20';

SELECT '-- python-0.25.5-checkpoint: checkpoint written by the delta-rs Python writer';
SELECT * FROM deltaLakeLocal('$DATA/python-0.25.5-checkpoint') ORDER BY ALL;

SELECT '-- spark-checkpoint-protection: checkpointProtection writer feature';
SELECT * FROM deltaLakeLocal('$DATA/spark-checkpoint-protection') ORDER BY ALL;

SELECT '-- table_failed_last_checkpoint_update: _last_checkpoint points to version 1, the newest checkpoint is version 3';
DESCRIBE deltaLakeLocal('$DATA/table_failed_last_checkpoint_update');
SELECT id, price, sold, deleted, count() FROM deltaLakeLocal('$DATA/table_failed_last_checkpoint_update') GROUP BY ALL ORDER BY ALL;
SELECT count() FROM deltaLakeLocal('$DATA/table_failed_last_checkpoint_update') SETTINGS delta_lake_snapshot_version = 0;
SELECT count() FROM deltaLakeLocal('$DATA/table_failed_last_checkpoint_update') SETTINGS delta_lake_snapshot_version = 1;
SELECT count() FROM deltaLakeLocal('$DATA/table_failed_last_checkpoint_update') SETTINGS delta_lake_snapshot_version = 2;

SELECT '-- delta-1.2.1-only-struct-stats: checkpoint with stats_parsed only, all types';
DESCRIBE deltaLakeLocal('$DATA/delta-1.2.1-only-struct-stats');
SELECT * FROM deltaLakeLocal('$DATA/delta-1.2.1-only-struct-stats') ORDER BY integer;

SELECT '-- delta-stats-optional: no statistics in the JSON log';
SELECT * FROM deltaLakeLocal('$DATA/delta-stats-optional') ORDER BY integer;

SELECT '-- delta-checkpoint-stats-optional: no statistics in the checkpoint';
SELECT * FROM deltaLakeLocal('$DATA/delta-checkpoint-stats-optional') ORDER BY integer;

SELECT '-- table_with_null_stats_in_notnull_struct';
DESCRIBE deltaLakeLocal('$DATA/table_with_null_stats_in_notnull_struct');
SELECT * FROM deltaLakeLocal('$DATA/table_with_null_stats_in_notnull_struct') ORDER BY ALL;
"
