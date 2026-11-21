#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Format version 1 Iceberg tables written by Spark 2.4 and Hive 3.1 + Iceberg 0.11, from the Apache Impala
# test data, see `data_iceberg/impala/README.md`. The expected results are the ones of the Impala tests
# (`iceberg-query.test`, `iceberg-migrated-tables.test`, `iceberg-missing-field-ids.test`, ...).
#
# All of these tables have the legacy `schema` field without `schema-id` in the metadata and in the manifest
# headers, and snapshots without `schema-id`; some have no `sort-orders`.
# Until https://github.com/ClickHouse/ClickHouse/issues/129991 is fixed, they fail with `BAD_ARGUMENTS` or `POCO_EXCEPTION`
# (also the missing `schema-id` in the manifest headers and the missing `sort-orders`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/impala" || exit 1

$CLICKHOUSE_LOCAL --session_timezone UTC --ignore-error -q "
-- Spark 2.4 tables: partitioned by hour(event_time) and identity(action), not partitioned, and in a Hadoop catalog.
SELECT 'iceberg_partitioned';
SELECT * FROM icebergLocal('iceberg_partitioned') ORDER BY id;
SELECT count(), countIf(id > 10), countIf(action != 'click') FROM icebergLocal('iceberg_partitioned');
SELECT action, count() FROM icebergLocal('iceberg_partitioned') WHERE event_time >= '2020-01-01 10:00:00' GROUP BY action ORDER BY action;
SELECT 'iceberg_non_partitioned';
SELECT * FROM icebergLocal('iceberg_non_partitioned') ORDER BY id;
SELECT 'hadoop_catalog_test';
SELECT * FROM icebergLocal('hadoop_catalog/hadoop_catalog_test/functional_parquet/hadoop_catalog_test') ORDER BY id;

-- The same with ORC data files, partitioned by identity(action).
SELECT 'iceberg_partitioned_orc';
SELECT * FROM icebergLocal('hadoop_catalog/iceberg_partitioned_orc/functional_parquet/iceberg_partitioned_orc') ORDER BY id;
SELECT count(DISTINCT id), count(DISTINCT action) FROM icebergLocal('hadoop_catalog/iceberg_partitioned_orc/functional_parquet/iceberg_partitioned_orc');

-- Columns are resolved by field id: arrays, maps and nested structs.
SELECT 'iceberg_resolution_test';
SELECT * FROM icebergLocal('hadoop_catalog/iceberg_resolution_test/functional_parquet/iceberg_resolution_test') ORDER BY id;

-- Regular Parquet and ORC tables converted to Iceberg: the data files have no field ids, columns are matched by name.
SELECT 'airports_parquet';
SELECT count(), uniqExact(iata), countIf(state IS NULL) FROM icebergLocal('hadoop_catalog/ice/airports_parquet');
SELECT * FROM icebergLocal('hadoop_catalog/ice/airports_parquet') WHERE country != 'USA' ORDER BY ALL;
SELECT lat, lon, city, airport FROM icebergLocal('hadoop_catalog/ice/airports_parquet') WHERE iata = '00R';
SELECT 'airports_orc';
SELECT count(), uniqExact(iata), countIf(state IS NULL) FROM icebergLocal('hadoop_catalog/ice/airports_orc');
SELECT * FROM icebergLocal('hadoop_catalog/ice/airports_orc') WHERE country != 'USA' ORDER BY ALL;

-- Hive tables migrated to Iceberg, with identity partitions of all the primitive types.
SELECT 'iceberg_alltypes_part';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_alltypes_part') ORDER BY ALL;
SELECT i FROM icebergLocal('hadoop_catalog/ice/iceberg_alltypes_part') WHERE p_int = 1 AND p_string = 'impala' AND p_date = '2022-02-22' ORDER BY i;
SELECT count() FROM icebergLocal('hadoop_catalog/ice/iceberg_alltypes_part') WHERE p_int = 2;
SELECT 'iceberg_alltypes_part_orc';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_alltypes_part_orc') ORDER BY ALL;
SELECT i FROM icebergLocal('hadoop_catalog/ice/iceberg_alltypes_part_orc') WHERE p_bigint = 11 AND p_decimal = 123.321 ORDER BY i;

-- Partition columns promoted after the migration (int -> bigint, float -> double, decimal(5, 3) -> decimal(8, 3)),
-- and a non-partition column moved to the end of the schema.
SELECT 'iceberg_legacy_partition_schema_evolution';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_legacy_partition_schema_evolution') ORDER BY ALL;
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_legacy_partition_schema_evolution') WHERE p_dec_dec = 2.718;
SELECT 'iceberg_legacy_partition_schema_evolution_orc';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_legacy_partition_schema_evolution_orc') ORDER BY ALL;
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_legacy_partition_schema_evolution_orc') WHERE p_dec_dec = 2.718;

-- Hive tables partitioned by hour(ts), with timestamp and timestamptz columns, including values before 1970.
SELECT 'iceberg_timestamp_part';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_timestamp_part') ORDER BY i;
SELECT i FROM icebergLocal('hadoop_catalog/ice/iceberg_timestamp_part') WHERE ts = '1969-01-01 01:00:00';
SELECT i FROM icebergLocal('hadoop_catalog/ice/iceberg_timestamp_part') WHERE ts < '2021-01-01 00:00:00' ORDER BY i;
SELECT 'iceberg_timestamptz_part';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_timestamptz_part') ORDER BY i;
SELECT i FROM icebergLocal('hadoop_catalog/ice/iceberg_timestamptz_part') WHERE ts = '1969-01-01 00:00:00';
SELECT i, toTimeZone(ts, 'Europe/Budapest') FROM icebergLocal('hadoop_catalog/ice/iceberg_timestamptz_part') ORDER BY i;

-- Hive + Iceberg 0.11 ORC table with nested arrays, maps and structs.
SELECT 'complextypestbl_iceberg_orc';
SELECT * FROM icebergLocal('hadoop_catalog/ice/complextypestbl_iceberg_orc') ORDER BY id;
SELECT id, length(int_array), nested_struct.a FROM icebergLocal('hadoop_catalog/ice/complextypestbl_iceberg_orc') ORDER BY id;

-- The data files of the table are in three sibling directories outside of the table location (\`write.data.path\` was changed twice).
SELECT 'iceberg_multiple_storage_locations';
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_multiple_storage_locations') ORDER BY ALL;
SELECT * FROM icebergLocal('hadoop_catalog/ice/iceberg_multiple_storage_locations') WHERE col_int = 2 ORDER BY ALL;
SELECT col_int FROM icebergLocal('hadoop_catalog/ice/iceberg_multiple_storage_locations') WHERE col_string = 'a' AND col_int >= 0 ORDER BY ALL;
" 2>&1
