#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# Deletion vectors written by Spark (see `data_lake_corpus/manifest.jsonl` and `05560_delta_lake_corpus`) together
# with ClickHouse features that depend on which rows of a data file are read: count() with and without
# `optimize_count_from_files`, filters, the `_row_number`, `_file`, `_path` and
# `_data_lake_snapshot_version` virtual columns, tiny blocks, time travel, and the `DeltaLakeLocal` table engine.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

T="$CUR_DIR/data_lake_corpus/delta/deletion_vectors"
P="$CUR_DIR/data_lake_corpus/delta/deletion_vectors_partitioned_column_mapping"
M="$CUR_DIR/data_lake_corpus/delta/merge"
O="$CUR_DIR/data_lake_corpus/delta/delta332_deletion_vectors"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT '-- count';
SELECT count(), sum(id) FROM deltaLakeLocal('$T');
SELECT count(), sum(id) FROM deltaLakeLocal('$T') SETTINGS optimize_count_from_files = 0;
SELECT count() FROM deltaLakeLocal('$T') WHERE id % 10 = 0;
SELECT count() FROM deltaLakeLocal('$T') WHERE id >= 100;

SELECT '-- every version';
SELECT 0, count(), sum(id) FROM deltaLakeLocal('$T') SETTINGS delta_lake_snapshot_version = 0;
SELECT 1, count(), sum(id) FROM deltaLakeLocal('$T') SETTINGS delta_lake_snapshot_version = 1;
SELECT 2, count(), sum(id) FROM deltaLakeLocal('$T') SETTINGS delta_lake_snapshot_version = 2;
SELECT 3, count(), sum(id) FROM deltaLakeLocal('$T') SETTINGS delta_lake_snapshot_version = 3;
SELECT 4, count(), sum(id) FROM deltaLakeLocal('$T') SETTINGS delta_lake_snapshot_version = 4;
SELECT 5, count(), sum(id) FROM deltaLakeLocal('$T') SETTINGS delta_lake_snapshot_version = 5;
SELECT 6, count(), sum(id) FROM deltaLakeLocal('$T') SETTINGS delta_lake_snapshot_version = 6;
SELECT 6, count(), sum(id) FROM deltaLakeLocal('$T') SETTINGS delta_lake_snapshot_version = 6, optimize_count_from_files = 0;

SELECT '-- rows around deleted ones';
SELECT groupArray(id) FROM (SELECT id FROM deltaLakeLocal('$T') WHERE id BETWEEN 45 AND 65 ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM deltaLakeLocal('$T') WHERE s = 'updated' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM deltaLakeLocal('$T') WHERE s = 'updated' ORDER BY id) SETTINGS delta_lake_snapshot_version = 5;

SELECT '-- tiny blocks';
SELECT count(), sum(id), sum(length(s)) FROM deltaLakeLocal('$T') SETTINGS max_block_size = 3, input_format_parquet_max_block_size = 3, max_threads = 1;
SELECT count(), sum(id), sum(length(s)) FROM deltaLakeLocal('$T') SETTINGS max_block_size = 1000000, input_format_parquet_max_block_size = 1000000;

SELECT '-- virtual columns';
SELECT count(), uniqExact(_path), uniqExact(_file), min(_data_lake_snapshot_version), max(_data_lake_snapshot_version) FROM deltaLakeLocal('$T');
SELECT id, s, _row_number FROM deltaLakeLocal('$T') WHERE id IN (1, 2, 12, 101, 149) ORDER BY id;
SELECT count() FROM deltaLakeLocal('$T') WHERE _row_number = id;

SELECT '-- table engine';
CREATE TABLE dv ENGINE = DeltaLakeLocal('$T');
SELECT count(), sum(id) FROM dv;
SELECT count(), sum(id) FROM dv WHERE s LIKE 'v1%';
SELECT count(), sum(id) FROM dv SETTINGS delta_lake_snapshot_version = 2;

SELECT '-- older writer';
SELECT count(), sum(id), countIf(s = 'updated') FROM deltaLakeLocal('$O');
SELECT (SELECT groupArray((id, s, p)) FROM (SELECT * FROM deltaLakeLocal('$T') ORDER BY id))
     = (SELECT groupArray((id, s, p)) FROM (SELECT * FROM deltaLakeLocal('$O') ORDER BY id));

SELECT '-- partitioned, column mapping, renamed column';
SELECT p, count(), sum(id), countIf(s_renamed LIKE 'v%') FROM deltaLakeLocal('$P') GROUP BY p ORDER BY p;
SELECT count() FROM deltaLakeLocal('$P') WHERE p = 1;
SELECT count() FROM deltaLakeLocal('$P') WHERE p = 1 SETTINGS delta_lake_snapshot_version = 2;
SELECT groupArray(id) FROM (SELECT id FROM deltaLakeLocal('$P') WHERE p = 1 ORDER BY id);
DESCRIBE deltaLakeLocal('$P') SETTINGS delta_lake_snapshot_version = 2;

SELECT '-- MERGE';
SELECT s = 'merged', count(), sum(id), min(id), max(id) FROM deltaLakeLocal('$M') GROUP BY 1 ORDER BY 1;
SELECT groupArray(id) FROM (SELECT id FROM deltaLakeLocal('$M') WHERE id BETWEEN 18 AND 32 ORDER BY id);
"
