#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# Reading tables that use various reader and writer table features: identity columns, column
# invariants, column defaults, append-only, liquid clustering, UniForm (Iceberg v1 and v2, Hudi),
# a writer feature that is not supported for writes, and Change Data Feed written by Trino.
# The tables and the expected results come from the Trino Delta Lake connector tests,
# see `data_delta_lake/trino/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake/trino"

$CLICKHOUSE_LOCAL -q "
SELECT 'databricks122/identity_columns';
DESCRIBE deltaLakeLocal('$DATA/databricks122/identity_columns') SETTINGS describe_compact_output = 1;
SELECT count() FROM deltaLakeLocal('$DATA/databricks122/identity_columns');
SELECT 'databricks133/identity_columns_table_feature';
DESCRIBE deltaLakeLocal('$DATA/databricks133/identity_columns_table_feature') SETTINGS describe_compact_output = 1;
SELECT count() FROM deltaLakeLocal('$DATA/databricks133/identity_columns_table_feature');

SELECT 'databricks122/invariants_writer_feature';
DESCRIBE deltaLakeLocal('$DATA/databricks122/invariants_writer_feature') SETTINGS describe_compact_output = 1;
SELECT * FROM deltaLakeLocal('$DATA/databricks122/invariants_writer_feature');
SELECT 'deltalake/invariants';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/invariants');

SELECT 'deltalake/allow_column_defaults';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/allow_column_defaults');

SELECT 'deltalake/append_only';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/append_only') ORDER BY ALL;

SELECT 'deltalake/liquid_clustering';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/liquid_clustering') ORDER BY ALL;
SELECT data FROM deltaLakeLocal('$DATA/deltalake/liquid_clustering') WHERE month = 2;

-- Three files, one per year, each with 100 rows.
SELECT 'deltalake/liquid_clustering_multi_column';
SELECT year, count(), uniqExact(month), min(data), max(data) FROM deltaLakeLocal('$DATA/deltalake/liquid_clustering_multi_column') GROUP BY year ORDER BY year;
SELECT count() FROM deltaLakeLocal('$DATA/deltalake/liquid_clustering_multi_column') WHERE year = 2022 AND month = 3;
SELECT count(), uniqExact(_path) FROM deltaLakeLocal('$DATA/deltalake/liquid_clustering_multi_column') WHERE year >= 2022;

SELECT 'uniform';
SELECT * FROM deltaLakeLocal('$DATA/databricks133/uniform_iceberg_v1');
SELECT * FROM deltaLakeLocal('$DATA/databricks143/uniform_iceberg_v2');
SELECT * FROM deltaLakeLocal('$DATA/deltalake/uniform_hudi');

-- The table requires the generatedColumns writer feature; it can still be read.
SELECT 'deltalake/unsupported_writer_feature';
DESCRIBE deltaLakeLocal('$DATA/deltalake/unsupported_writer_feature') SETTINGS describe_compact_output = 1;
SELECT count() FROM deltaLakeLocal('$DATA/deltalake/unsupported_writer_feature');

-- Partitioned table with Change Data Feed written by Trino: the partition key of the row is updated.
SELECT 'trino/cdc_table';
SELECT * FROM deltaLakeLocal('$DATA/trino/cdc_table') ORDER BY ALL;
SELECT * FROM deltaLakeLocal('$DATA/trino/cdc_table') ORDER BY ALL SETTINGS delta_lake_snapshot_version = 0;
SELECT key, value, _change_type, _commit_version FROM deltaLakeLocal('$DATA/trino/cdc_table') ORDER BY _commit_version, _change_type
SETTINGS delta_lake_snapshot_start_version = 0, delta_lake_snapshot_end_version = 1;
"
