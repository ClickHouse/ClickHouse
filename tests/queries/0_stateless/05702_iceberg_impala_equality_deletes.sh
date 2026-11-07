#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Format version 2 Iceberg tables with equality delete files (and some with position delete files too) written
# by Flink, NiFi and Impala, from the Apache Impala test data, see `data_iceberg/impala/README.md`.
# The expected results are the ones of the Impala test `iceberg-v2-read-equality-deletes.test`.
#
# The manifests have no `partition-spec` and `schema` in the Avro header, until
# https://github.com/ClickHouse/ClickHouse/issues/129989 is fixed the tables fail with `ICEBERG_SPECIFICATION_VIOLATION`.
# The data files are referenced as `/test-warehouse/...` in the manifests, but as `hdfs://localhost:20500/test-warehouse/...`
# in the position delete files: both are the same file in the table location, but the position deletes are currently
# not applied because the paths are compared as strings: https://github.com/ClickHouse/ClickHouse/issues/129990

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/impala" || exit 1

$CLICKHOUSE_LOCAL --session_timezone UTC --ignore-error -q "
-- A row updated through an equality delete on the identifier field.
SELECT 'iceberg_v2_delete_equality';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_equality') ORDER BY ALL;
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_equality') ORDER BY ALL SETTINGS iceberg_snapshot_id = 5763349507283783091;

-- The equality delete file with data sequence number 2 contains NULL and 3: it drops the matching rows with a lower
-- data sequence number (including the row with NULL) and keeps the ones with a higher data sequence number.
SELECT 'iceberg_v2_delete_equality_nulls';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_equality_nulls') ORDER BY ALL;
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_equality_nulls') ORDER BY ALL SETTINGS iceberg_snapshot_id = 4346796256488077976;

-- Both equality and position delete files.
SELECT 'iceberg_v2_delete_both_eq_and_pos';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_both_eq_and_pos') ORDER BY ALL;

-- Equality delete files in both partitions of a table partitioned by identity(d).
SELECT 'iceberg_v2_delete_equality_partitioned';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_equality_partitioned') ORDER BY d, s;
SELECT count() FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_equality_partitioned') WHERE d = '2023-12-25';

-- Equality delete files with different equality field ids.
SELECT 'iceberg_v2_delete_equality_multi_eq_ids';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_equality_multi_eq_ids') ORDER BY ALL;
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_equality_multi_eq_ids') ORDER BY ALL SETTINGS iceberg_snapshot_id = 8127619959873391049;

-- Position delete files and equality delete files with different equality field ids.
SELECT 'iceberg_v2_delete_pos_and_multi_eq_ids';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_delete_pos_and_multi_eq_ids') ORDER BY ALL;

-- Schema evolution (a column added) after equality deletes on the identifier field.
SELECT 'iceberg_v2_equality_delete_schema_evolution';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_equality_delete_schema_evolution') ORDER BY ALL;
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_v2_equality_delete_schema_evolution') ORDER BY ALL SETTINGS iceberg_snapshot_id = 3986738438831924669;
" 2>&1
