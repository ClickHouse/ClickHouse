#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg (Avro) is not in fast test

# Identity partitions of every primitive type, partition evolution, and identity partition
# sources that are missing from the data files.
# The tables and the expected results come from the DuckDB Iceberg extension tests
# (`test/sql/local/partitioning/identity/*.test`), see `data_iceberg/duckdb/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/duckdb"

# The table locations are relative (`data/persistent/<table>`), so the queries run from the directory containing `data`.
cd "$DATA" || exit 1

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT 'partition_bigint';
SELECT * FROM icebergLocal('data/persistent/partition_bigint') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_bigint') WHERE partition_col = 9223372036854775807;

SELECT 'partition_binary';
SELECT hex(partition_col), user_id, event_type FROM icebergLocal('data/persistent/partition_binary') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_binary') WHERE partition_col = unhex('010203');
SELECT user_id FROM icebergLocal('data/persistent/partition_binary') WHERE partition_col = unhex('AABBCC');

SELECT 'partition_bool';
SELECT * FROM icebergLocal('data/persistent/partition_bool') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_bool') WHERE partition_col = true;

SELECT 'partition_decimal_smallint';
SELECT toTypeName(partition_col) FROM icebergLocal('data/persistent/partition_decimal_smallint') LIMIT 1;
SELECT * FROM icebergLocal('data/persistent/partition_decimal_smallint') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_decimal_smallint') WHERE partition_col = 1.23;

SELECT 'partition_decimal_integer';
SELECT toTypeName(partition_col) FROM icebergLocal('data/persistent/partition_decimal_integer') LIMIT 1;
SELECT * FROM icebergLocal('data/persistent/partition_decimal_integer') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_decimal_integer') WHERE partition_col = 42.5630;

SELECT 'partition_decimal_bigint';
SELECT toTypeName(partition_col) FROM icebergLocal('data/persistent/partition_decimal_bigint') LIMIT 1;
SELECT * FROM icebergLocal('data/persistent/partition_decimal_bigint') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_decimal_bigint') WHERE partition_col = toDecimal64('123.45678901234', 11);

SELECT 'partition_decimal_hugeint';
SELECT toTypeName(partition_col) FROM icebergLocal('data/persistent/partition_decimal_hugeint') LIMIT 1;
SELECT * FROM icebergLocal('data/persistent/partition_decimal_hugeint') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_decimal_hugeint') WHERE partition_col = toDecimal128('3.14159265358979323846264', 23);

-- Both tables have an Iceberg 'float' column (the DuckDB test of partition_double prints it as a double).
SELECT 'partition_double';
SELECT * FROM icebergLocal('data/persistent/partition_double') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_double') WHERE partition_col = toFloat32(3.141592653589793);

SELECT 'partition_float';
SELECT * FROM icebergLocal('data/persistent/partition_float') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_float') WHERE partition_col = toFloat32(1.23);

SELECT 'partition_integer';
SELECT * FROM icebergLocal('data/persistent/partition_integer') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_integer') WHERE partition_col = 42;

-- First partitioned by 'event_date', later by 'event_date' and 'event_type'.
SELECT 'hive_partitioned_table';
SELECT * FROM icebergLocal('data/persistent/hive_partitioned_table') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/hive_partitioned_table') WHERE event_date = '2024-01-02';
SELECT user_id FROM icebergLocal('data/persistent/hive_partitioned_table') WHERE event_type = 'view' ORDER BY ALL;

-- identity(region) and bucket(16, region) share the source column, which is missing from the data files.
SELECT 'partition_values_missing_from_files/shared_source';
SELECT id, region FROM icebergLocal('data/persistent/partition_values_missing_from_files/warehouse/default/shared_source') ORDER BY id;
SELECT count() FROM icebergLocal('data/persistent/partition_values_missing_from_files/warehouse/default/shared_source') WHERE region = 'eu';
"

# The identity source is a field of a struct, and the struct in the data files does not have it.
# Bug nested-identity-source: the field is read as NULL instead of the partition value.
$CLICKHOUSE_LOCAL -q "
SELECT 'partition_values_missing_from_files/nested_source';
SELECT id, address FROM icebergLocal('data/persistent/partition_values_missing_from_files/warehouse/default/nested_source') ORDER BY id;
SELECT count() FROM icebergLocal('data/persistent/partition_values_missing_from_files/warehouse/default/nested_source') WHERE address.country = 'fr';
"

# Identity partitions across files and spec evolution, with nested sources, NULL partition values,
# and `initial-default` values for fields that are not partition sources.
# Bugs metadata-file-names (`final.metadata.json`), nested-identity-source and initial-default.
# A NULL struct is read as a tuple of NULLs in ClickHouse.
MIXED="icebergLocal('data/persistent/partition_values_missing_edge_cases/default/mixed', SETTINGS iceberg_metadata_file_path = 'metadata/final.metadata.json')"
$CLICKHOUSE_LOCAL -q "
SELECT 'partition_values_missing_edge_cases/mixed';
SELECT id, region, address FROM $MIXED ORDER BY id;
SELECT address.geo FROM $MIXED WHERE id IN (7, 8) ORDER BY id;
SELECT count(), sum(id) FROM $MIXED WHERE address.country = 'fr';
SELECT count(), sum(id) FROM $MIXED WHERE address.geo.code = 84;
SELECT count(), sum(id) FROM $MIXED WHERE region IS NULL;
SELECT count(), sum(id) FROM $MIXED WHERE address.country IS NULL AND address.geo.code IS NULL;
SELECT id FROM $MIXED WHERE region = 'unknown-region' AND address.country = 'unknown-country';
SELECT count(), min(extra) FROM $MIXED;
"

# Tables with partition values that need escaping in file names are stored in a tar archive.
EXTRACTED="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$EXTRACTED"
mkdir -p "$EXTRACTED"
tar -xf "$DATA/special_paths.tar" -C "$EXTRACTED"
cd "$EXTRACTED" || exit 1

# Iceberg 'time' is read as Int64 microseconds since midnight.
$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT 'partition_time';
SELECT * FROM icebergLocal('data/persistent/partition_time/default.db/partition_time') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_time/default.db/partition_time') WHERE partition_col = 45296000000;

SELECT 'partition_timestamp';
SELECT * FROM icebergLocal('data/persistent/partition_timestamp/default.db/partition_timestamp') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_timestamp/default.db/partition_timestamp') WHERE partition_col = '2023-05-15 14:30:45';

SELECT 'partition_timestamptz';
SELECT * FROM icebergLocal('data/persistent/partition_timestamptz/default.db/partition_timestamptz') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_timestamptz/default.db/partition_timestamptz') WHERE partition_col = '2023-05-15 14:30:45';

SELECT 'partition_fixed_length_binary';
SELECT toTypeName(partition_col), length(partition_col) FROM icebergLocal('data/persistent/partition_fixed_length_binary/default.db/partition_fixed_length_binary') LIMIT 1;
SELECT * FROM icebergLocal('data/persistent/partition_fixed_length_binary/default.db/partition_fixed_length_binary') ORDER BY ALL;
SELECT user_id FROM icebergLocal('data/persistent/partition_fixed_length_binary/default.db/partition_fixed_length_binary') WHERE partition_col = 'hello world';
SELECT user_id FROM icebergLocal('data/persistent/partition_fixed_length_binary/default.db/partition_fixed_length_binary') WHERE partition_col = 'fixed value';

-- Written by Java Iceberg: v3 identity partition on a 'timestamptz_ns' column, including values before the epoch and NULL.
SELECT 'partition_timestamptz_ns';
SELECT toTypeName(ts) FROM icebergLocal('data/persistent/partition_timestamptz_ns') LIMIT 1;
SELECT id, toUnixTimestamp64Nano(ts) FROM icebergLocal('data/persistent/partition_timestamptz_ns') ORDER BY id;
SELECT id FROM icebergLocal('data/persistent/partition_timestamptz_ns') WHERE ts = toDateTime64('2026-10-09 10:00:00.123456789', 9, 'UTC');
SELECT id FROM icebergLocal('data/persistent/partition_timestamptz_ns') WHERE ts = toDateTime64('2026-10-09 12:00:00.123456790', 9, 'Europe/Amsterdam');
SELECT id FROM icebergLocal('data/persistent/partition_timestamptz_ns') WHERE ts < toDateTime64('1970-01-01 00:00:00', 9, 'UTC');
SELECT id FROM icebergLocal('data/persistent/partition_timestamptz_ns') WHERE ts IS NULL;
SELECT toTimeZone(ts, 'Europe/Amsterdam') FROM icebergLocal('data/persistent/partition_timestamptz_ns') WHERE id = 1;
"

cd "$CUR_DIR" || exit 1
rm -rf "$EXTRACTED"
