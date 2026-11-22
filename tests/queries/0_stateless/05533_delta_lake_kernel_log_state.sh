#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Log replay and table state over the tables of delta-kernel-rs `kernel/tests/data` (see
# `data_delta_lake/kernel_data/README.md`): set-transaction (`txn`) actions with and without a
# checkpoint, version checksum (`.crc`) files, a checkpoint without `_last_checkpoint`, partition
# columns with column mapping, the `materializePartitionColumns` writer feature, a table whose
# data files differ in nullability from the table schema, and all primitive and nested types.
# Some tables are kernel fixtures with a transaction log and no data files: for them, only the
# schema and `count()`, which is answered from the `numRecords` statistics, are checked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="${CUR_DIR}/data_delta_lake/kernel_data"

$CLICKHOUSE_LOCAL -q "
SET session_timezone = 'UTC';

SELECT '--- app-txn-no-checkpoint and app-txn-checkpoint: the same commits without and with a checkpoint';
SELECT id, modified, count(), sum(value) FROM deltaLakeLocal('${DATA}/app-txn-no-checkpoint') GROUP BY ALL ORDER BY ALL;
SELECT id, modified, count(), sum(value) FROM deltaLakeLocal('${DATA}/app-txn-checkpoint') GROUP BY ALL ORDER BY ALL;
SELECT '--- app-txn-checkpoint: time travel before the checkpoint';
SELECT count() FROM deltaLakeLocal('${DATA}/app-txn-checkpoint') SETTINGS delta_lake_snapshot_version = 0;
SELECT count() FROM deltaLakeLocal('${DATA}/app-txn-checkpoint') SETTINGS delta_lake_snapshot_version = 1;

SELECT '--- app-txn-with-last-updated: log without data files';
DESCRIBE deltaLakeLocal('${DATA}/app-txn-with-last-updated');
SELECT count() FROM deltaLakeLocal('${DATA}/app-txn-with-last-updated');
SELECT count() FROM deltaLakeLocal('${DATA}/app-txn-with-last-updated') SETTINGS delta_lake_snapshot_version = 1;

SELECT '--- with_checkpoint_no_last_checkpoint: a checkpoint found without _last_checkpoint, log without data files';
DESCRIBE deltaLakeLocal('${DATA}/with_checkpoint_no_last_checkpoint');
SELECT count() FROM deltaLakeLocal('${DATA}/with_checkpoint_no_last_checkpoint');

SELECT '--- crc-full: a table with a version checksum file';
DESCRIBE deltaLakeLocal('${DATA}/crc-full');
SELECT count() FROM deltaLakeLocal('${DATA}/crc-full');

SELECT '--- partition_cm: a partition column without column mapping, with name and with id mode';
DESCRIBE deltaLakeLocal('${DATA}/partition_cm/none');
DESCRIBE deltaLakeLocal('${DATA}/partition_cm/name');
DESCRIBE deltaLakeLocal('${DATA}/partition_cm/id');
SELECT count() FROM deltaLakeLocal('${DATA}/partition_cm/name');

SELECT '--- partitioned_with_materialize_feature: partition values also stored in the data files';
DESCRIBE deltaLakeLocal('${DATA}/partitioned_with_materialize_feature');
SELECT * FROM deltaLakeLocal('${DATA}/partitioned_with_materialize_feature') ORDER BY number;
SELECT number FROM deltaLakeLocal('${DATA}/partitioned_with_materialize_feature') WHERE letter IS NULL;
SELECT arraySort(groupArray(number)) FROM deltaLakeLocal('${DATA}/partitioned_with_materialize_feature') WHERE letter = 'a';

SELECT '--- external-table-different-nullability';
DESCRIBE deltaLakeLocal('${DATA}/external-table-different-nullability');
SELECT * FROM deltaLakeLocal('${DATA}/external-table-different-nullability') ORDER BY i;

SELECT '--- stats-writing-all-types: all primitive and nested types';
DESCRIBE deltaLakeLocal('${DATA}/stats-writing-all-types/delta');
SELECT * FROM deltaLakeLocal('${DATA}/stats-writing-all-types/delta') ORDER BY byte_col FORMAT TSVWithNames;
SELECT byte_col FROM deltaLakeLocal('${DATA}/stats-writing-all-types/delta') WHERE timestamp_col > '2023-12-31 23:59:58' ORDER BY ALL;
SELECT byte_col FROM deltaLakeLocal('${DATA}/stats-writing-all-types/delta') WHERE decimal_col BETWEEN 400 AND 500;
SELECT byte_col FROM deltaLakeLocal('${DATA}/stats-writing-all-types/delta') WHERE nested_struct.2 = 'nested_c';
"
