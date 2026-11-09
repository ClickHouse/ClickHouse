#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# Transaction log replay: classic, multi-part and V2 checkpoints (JSON and Parquet, with sidecars),
# logs with missing old commits, version checksum (`.crc`) files with edge cases,
# in-commit timestamps, and time travel across checkpoints and schema changes.
# The tables and the expected results come from the Trino Delta Lake connector tests,
# see `data_delta_lake/trino/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake/trino"

$CLICKHOUSE_LOCAL -q "
SELECT 'simple_table_without_checkpoint', groupArray(a_column) FROM (SELECT a_column FROM deltaLakeLocal('$DATA/databricks73/simple_table_without_checkpoint') ORDER BY ALL);
SELECT 'simple_table_ending_on_checkpoint', groupArray(a_column) FROM (SELECT a_column FROM deltaLakeLocal('$DATA/databricks73/simple_table_ending_on_checkpoint') ORDER BY ALL);
SELECT 'simple_table_past_checkpoint', groupArray(a_column) FROM (SELECT a_column FROM deltaLakeLocal('$DATA/databricks73/simple_table_past_checkpoint') ORDER BY ALL);

SELECT 'multipart_checkpoint';
SELECT groupArray(c) FROM (SELECT c FROM deltaLakeLocal('$DATA/deltalake/multipart_checkpoint') ORDER BY c);
SELECT groupArray(c) FROM (SELECT c FROM deltaLakeLocal('$DATA/deltalake/multipart_checkpoint') ORDER BY c) SETTINGS delta_lake_snapshot_version = 5;
SELECT groupArray(c) FROM (SELECT c FROM deltaLakeLocal('$DATA/deltalake/multipart_checkpoint') ORDER BY c) SETTINGS delta_lake_snapshot_version = 6;
SELECT groupArray(c) FROM (SELECT c FROM deltaLakeLocal('$DATA/deltalake/multipart_checkpoint') ORDER BY c) SETTINGS delta_lake_snapshot_version = 7;

SELECT 'multipart_v2_checkpoint';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/multipart_v2_checkpoint') ORDER BY ALL;

SELECT 'v2_checkpoint (Databricks)';
SELECT * FROM deltaLakeLocal('$DATA/databricks133/v2_checkpoint_json') ORDER BY ALL;
SELECT * FROM deltaLakeLocal('$DATA/databricks133/v2_checkpoint_parquet') ORDER BY ALL;
SELECT 'v2_checkpoint (OSS Delta Lake)';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/v2_checkpoint_json') ORDER BY ALL;
SELECT * FROM deltaLakeLocal('$DATA/deltalake/v2_checkpoint_parquet') ORDER BY ALL;

-- The commits before the V2 checkpoint are removed from the log.
SELECT 'v2_checkpoint_using_temporal';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/v2_checkpoint_json_using_temporal') ORDER BY ALL;
SELECT * FROM deltaLakeLocal('$DATA/deltalake/v2_checkpoint_parquet_using_temporal') ORDER BY ALL;

-- Metadata and protocol can be read from version checksum files; malformed ones must be ignored.
SELECT 'checksum';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/checksum');
SELECT * FROM deltaLakeLocal('$DATA/deltalake/checksum_invalid_json');
SELECT * FROM deltaLakeLocal('$DATA/deltalake/checksum_invalid_json_mapping');
SELECT * FROM deltaLakeLocal('$DATA/deltalake/checksum_missing_latest');
SELECT * FROM deltaLakeLocal('$DATA/deltalake/checksum_trailing_json_content');
SELECT * FROM deltaLakeLocal('$DATA/deltalake/checksum_without_metadata');
SELECT * FROM deltaLakeLocal('$DATA/deltalake/checksum_without_protocol');

SELECT 'in_commit_timestamp_history_read';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/in_commit_timestamp_history_read') ORDER BY ALL;

-- Written by Trino 440 with checkpoint_interval = 2.
SELECT 'trino440/time_travel';
SELECT groupArray(id) FROM (SELECT id FROM deltaLakeLocal('$DATA/trino440/time_travel') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM deltaLakeLocal('$DATA/trino440/time_travel') ORDER BY id) SETTINGS delta_lake_snapshot_version = 0;
SELECT groupArray(id) FROM (SELECT id FROM deltaLakeLocal('$DATA/trino440/time_travel') ORDER BY id) SETTINGS delta_lake_snapshot_version = 1;
SELECT groupArray(id) FROM (SELECT id FROM deltaLakeLocal('$DATA/trino440/time_travel') ORDER BY id) SETTINGS delta_lake_snapshot_version = 2;

-- Databricks writes a checkpoint on every schema change; time travel crosses them.
SELECT 'test_write_checkpoint_on_schema_change';
SELECT * FROM deltaLakeLocal('$DATA/databricks164/test_write_checkpoint_on_schema_change') SETTINGS delta_lake_snapshot_version = 4;
SELECT * FROM deltaLakeLocal('$DATA/databricks164/test_write_checkpoint_on_schema_change') SETTINGS delta_lake_snapshot_version = 6;
SELECT * FROM deltaLakeLocal('$DATA/databricks164/test_write_checkpoint_on_schema_change') SETTINGS delta_lake_snapshot_version = 8;
SELECT * FROM deltaLakeLocal('$DATA/databricks164/test_write_checkpoint_on_schema_change') SETTINGS delta_lake_snapshot_version = 10;
SELECT * FROM deltaLakeLocal('$DATA/databricks164/test_write_checkpoint_on_schema_change') SETTINGS delta_lake_snapshot_version = 12;
SELECT * FROM deltaLakeLocal('$DATA/databricks164/test_write_checkpoint_on_schema_change');
"
