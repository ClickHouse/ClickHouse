#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Format version 2 Iceberg tables with position delete files written by Hive and Spark, from the Apache Impala
# test data, see `data_iceberg/impala/README.md`. The expected results are the ones of the Impala tests
# (`iceberg-v2-read-position-deletes.test`, `iceberg-v2-read-position-deletes-orc.test`, `test_iceberg.py`).
#
# The manifests have no `partition-spec` and `schema` in the Avro header (Impala's test data loader rewrites
# them with a plain Avro writer), until https://github.com/ClickHouse/ClickHouse/issues/129989 is fixed
# the tables fail with `ICEBERG_SPECIFICATION_VIOLATION`.
# The data files are referenced as `/test-warehouse/...` in the manifests, but as `hdfs://localhost:20500/test-warehouse/...`
# in the position delete files: both are the same file in the table location, but the position deletes are currently
# not applied because the paths are compared as strings: https://github.com/ClickHouse/ClickHouse/issues/129990
# Position deletes for ORC data files are not supported yet: https://github.com/ClickHouse/ClickHouse/issues/130001

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/impala" || exit 1

$CLICKHOUSE_LOCAL --session_timezone UTC --ignore-error -q "
-- One of three rows deleted.
SELECT 'iceberg_v2_delete_positional';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_positional') ORDER BY ALL;
SELECT count() FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_positional') WHERE id = 2;
SELECT data FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_positional')
WHERE data NOT IN (SELECT min(data) FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_positional'));

-- All rows deleted.
SELECT 'iceberg_v2_positional_delete_all_rows';
SELECT count() FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_positional_delete_all_rows');
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_positional_delete_all_rows') WHERE i > 2;
SELECT 'iceberg_v2_positional_delete_all_rows_orc';
SELECT count() FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_positional_delete_all_rows_orc');
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_positional_delete_all_rows_orc') WHERE i > 2;

-- All rows deleted and inserted again with new values.
SELECT 'iceberg_v2_positional_update_all_rows';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_positional_update_all_rows') ORDER BY ALL;
SELECT count() FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_positional_update_all_rows') WHERE i = 3;

-- Only some of the data files have delete files.
SELECT 'iceberg_v2_positional_not_all_data_files_have_delete_files';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_positional_not_all_data_files_have_delete_files') ORDER BY ALL;
SELECT i + 1000, upper(s) FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_positional_not_all_data_files_have_delete_files') WHERE i > 3 ORDER BY ALL;
SELECT 'iceberg_v2_positional_not_all_data_files_have_delete_files_orc';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_positional_not_all_data_files_have_delete_files_orc') ORDER BY ALL;
SELECT count() FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_positional_not_all_data_files_have_delete_files_orc') WHERE i < 4;

-- Partitioned by identity(action), a position delete file per partition.
SELECT 'iceberg_v2_partitioned_position_deletes';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_partitioned_position_deletes') ORDER BY ALL;
SELECT action, count() FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_partitioned_position_deletes') GROUP BY action ORDER BY action;
SELECT 'iceberg_v2_partitioned_position_deletes_orc';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_partitioned_position_deletes_orc') ORDER BY ALL;
SELECT action, count() FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_partitioned_position_deletes_orc') GROUP BY action ORDER BY action;
SELECT id FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_partitioned_position_deletes_orc') WHERE action = 'click' ORDER BY id;

-- Spark compaction (rewrite_data_files) left a dangling position delete file that refers to a removed data file.
SELECT 'iceberg_spark_compaction_with_dangling_delete';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_spark_compaction_with_dangling_delete') ORDER BY ALL;
SELECT count() FROM icebergLocal('hadoop_catalog/ice/iceberg_spark_compaction_with_dangling_delete') SETTINGS optimize_count_from_files = 0;

-- A data file with several row groups (TPC-H lineitem) where every fifth line number is deleted.
SELECT 'iceberg_lineitem_multiblock';
SELECT count(), countIf(l_linenumber % 5 = 0) FROM icebergLocal('hadoop_catalog/ice/iceberg_lineitem_multiblock') SETTINGS optimize_count_from_files = 0;
SELECT count() FROM icebergLocal('hadoop_catalog/ice/iceberg_lineitem_multiblock') SETTINGS max_block_size = 2, optimize_count_from_files = 0;
" 2>&1
