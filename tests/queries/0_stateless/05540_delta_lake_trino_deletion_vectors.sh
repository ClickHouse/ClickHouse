#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# Deletion vectors in Delta tables written by Databricks and OSS Delta Lake.
# The tables and the expected results come from the Trino Delta Lake connector tests,
# see `data_delta_lake/trino/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake/trino"

$CLICKHOUSE_LOCAL -q "
SELECT 'databricks122/deletion_vectors';
SELECT * FROM deltaLakeLocal('$DATA/databricks122/deletion_vectors') ORDER BY ALL;

-- A table with deletion vectors in its log, written while the table property was unset or disabled.
SELECT 'deltalake/deletion_vectors_unset';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/deletion_vectors_unset') ORDER BY ALL;
SELECT 'deltalake/deletion_vectors_disabled';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/deletion_vectors_disabled') ORDER BY ALL;
SELECT 'deltalake/deletion_vectors_disabled_cdf';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/deletion_vectors_disabled_cdf') ORDER BY ALL;
SELECT 'deltalake/deletion_vectors_disabled_checkpoint';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/deletion_vectors_disabled_checkpoint') ORDER BY ALL;

-- 20001 rows over two Parquet pages; the row in the second page is updated through a deletion vector.
SELECT 'deltalake/deletion_vector_pages';
SELECT count(), sum(id) FROM deltaLakeLocal('$DATA/deltalake/deletion_vector_pages');
SELECT count() FROM deltaLakeLocal('$DATA/deltalake/deletion_vector_pages') WHERE id = 20001;
SELECT id FROM deltaLakeLocal('$DATA/deltalake/deletion_vector_pages') WHERE id = 99999;
SELECT id FROM deltaLakeLocal('$DATA/deltalake/deletion_vector_pages') WHERE id IN (20000, 20001, 99999) ORDER BY id;
SELECT count() FROM deltaLakeLocal('$DATA/deltalake/deletion_vector_pages') WHERE id = 20001 SETTINGS input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0;

-- Change Data Feed of the same table.
SELECT id, _change_type, _commit_version FROM deltaLakeLocal('$DATA/deltalake/deletion_vector_pages') WHERE id = 20001 ORDER BY ALL
SETTINGS delta_lake_snapshot_start_version = 0, delta_lake_snapshot_end_version = 2;
"
