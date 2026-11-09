#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Various format version 2 Iceberg tables from the Apache Impala test data, see `data_iceberg/impala/README.md`.
# The expected results are the ones of the Impala tests (`iceberg-query.test`, `iceberg-column-case-sensitivity-issue.test`,
# `iceberg-migrated-table-field-id-resolution*.test`, `iceberg-tablesample-v2.test`), or of PyIceberg for the tables
# that the Impala tests only read after modifying them.
#
# The manifests have no `partition-spec` and `schema` in the Avro header, until
# https://github.com/ClickHouse/ClickHouse/issues/129989 is fixed the tables fail with `ICEBERG_SPECIFICATION_VIOLATION`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/impala" || exit 1

$CLICKHOUSE_LOCAL --session_timezone UTC --ignore-error -q "
-- Tables without delete files, with Parquet and ORC data files.
SELECT 'iceberg_v2_no_deletes';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_no_deletes') ORDER BY ALL;
SELECT sum(i), count() FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_no_deletes');
SELECT 'iceberg_v2_no_deletes_orc';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_no_deletes_orc') ORDER BY ALL;
SELECT sum(i), count() FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_no_deletes_orc');

-- The data file entries have \`key_metadata\` set.
SELECT 'iceberg_with_key_metadata';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_with_key_metadata') ORDER BY ALL;

-- An empty table with upper case column names, including nested fields.
SELECT 'iceberg_uppercase_col';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_uppercase_col');
SELECT count() FROM icebergLocal('hadoop_catalog/ice/iceberg_uppercase_col');

-- Upper case column names, partitioned by bucket(ID), with Impala data files.
SELECT 'iceberg_column_case_sensitivity_issue';
SELECT * FROM icebergLocal('iceberg_column_case_sensitivity_issue') ORDER BY ALL;
SELECT * FROM icebergLocal('iceberg_column_case_sensitivity_issue') WHERE ID = '1';

-- Hive tables migrated to Iceberg: the data files have no field ids.
SELECT 'iceberg_migrated_alter_test';
SELECT * FROM icebergLocal('iceberg_migrated_alter_test') ORDER BY ALL;
SELECT 'iceberg_migrated_alter_test_orc';
SELECT * FROM icebergLocal('iceberg_migrated_alter_test_orc') ORDER BY ALL;
SELECT 'iceberg_migrated_complex_test';
SELECT * FROM icebergLocal('iceberg_migrated_complex_test');
SELECT struct_1_col.string_col, struct_2_col.struct_3_col.float_col FROM icebergLocal('iceberg_migrated_complex_test');
SELECT 'iceberg_migrated_complex_test_orc';
SELECT * FROM icebergLocal('iceberg_migrated_complex_test_orc');
SELECT struct_1_col.string_col, struct_2_col.struct_3_col.float_col FROM icebergLocal('iceberg_migrated_complex_test_orc');

-- Data files in Parquet and ORC in the same table.
SELECT 'iceberg_mixed_file_format_test';
SELECT * FROM icebergLocal('iceberg_mixed_file_format_test') ORDER BY ALL;

-- A table with a Puffin statistics file written by Trino.
SELECT 'iceberg_with_puffin_stats';
SELECT * FROM icebergLocal('iceberg_with_puffin_stats') ORDER BY ALL;
" 2>&1
