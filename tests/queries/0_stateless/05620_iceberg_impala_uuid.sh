#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Iceberg `uuid` columns in Parquet, ORC and Avro data files and as identity and bucket partition sources, written by Spark,
# from the Apache Impala test data, see `data_iceberg/impala/README.md`. The expected results are the ones of the Impala tests
# `iceberg-uuid-type.test` and `iceberg-uuid-aggregates.test` (for Avro, which Impala does not read, the values decoded from the data files).
#
# Bugs, until they are fixed:
# - ORC `binary(16)` uuid values are read as NULL: https://github.com/ClickHouse/ClickHouse/issues/129993
# - Avro `fixed(16)` uuid values are rejected with `CANNOT_PARSE_UUID`: https://github.com/ClickHouse/ClickHouse/issues/129994
# - The uuid identity partition values are not parsed: https://github.com/ClickHouse/ClickHouse/issues/123961

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/impala" || exit 1

$CLICKHOUSE_LOCAL --session_timezone UTC --ignore-error -q "
SELECT 'iceberg_uuid_test';
SELECT id, uuid_col, name FROM icebergLocal('iceberg_uuid/iceberg_uuid_test') ORDER BY id;
SELECT id FROM icebergLocal('iceberg_uuid/iceberg_uuid_test') WHERE uuid_col = '12345678-1234-5678-1234-567812345678';
SELECT id FROM icebergLocal('iceberg_uuid/iceberg_uuid_test') WHERE uuid_col IN ('abcdefab-cdef-abcd-efab-cdefabcdefab', 'ffffffff-ffff-ffff-ffff-ffffffffffff') ORDER BY id;
SELECT id FROM icebergLocal('iceberg_uuid/iceberg_uuid_test') WHERE uuid_col IS NULL;
SELECT count(), count(uuid_col), uniqExact(uuid_col) FROM icebergLocal('iceberg_uuid/iceberg_uuid_test');

SELECT 'iceberg_uuid_test_orc';
SELECT id, uuid_col, name FROM icebergLocal('iceberg_uuid/iceberg_uuid_test_orc') ORDER BY id;
SELECT id FROM icebergLocal('iceberg_uuid/iceberg_uuid_test_orc') WHERE uuid_col = '12345678-1234-5678-1234-567812345678';
SELECT id FROM icebergLocal('iceberg_uuid/iceberg_uuid_test_orc') WHERE uuid_col IN ('abcdefab-cdef-abcd-efab-cdefabcdefab', 'ffffffff-ffff-ffff-ffff-ffffffffffff') ORDER BY id;
SELECT id FROM icebergLocal('iceberg_uuid/iceberg_uuid_test_orc') WHERE uuid_col IS NULL;

SELECT 'iceberg_uuid_test_avro';
SELECT id, uuid_col, name FROM icebergLocal('iceberg_uuid/iceberg_uuid_test_avro') ORDER BY id;
SELECT id FROM icebergLocal('iceberg_uuid/iceberg_uuid_test_avro') WHERE uuid_col = '12345678-1234-5678-1234-567812345678';

-- Partitioned by identity(uuid_identity) and bucket(uuid_bucket).
SELECT 'iceberg_uuid_test_part';
SELECT id, uuid_identity, uuid_bucket FROM icebergLocal('iceberg_uuid/iceberg_uuid_test_part') ORDER BY id;
SELECT id FROM icebergLocal('iceberg_uuid/iceberg_uuid_test_part') WHERE uuid_identity = '12345678-1234-5678-1234-567812345678' ORDER BY id;
SELECT id FROM icebergLocal('iceberg_uuid/iceberg_uuid_test_part') WHERE uuid_identity IS NULL;
SELECT id FROM icebergLocal('iceberg_uuid/iceberg_uuid_test_part') WHERE uuid_bucket = '12345678-1234-5678-1234-567812345678';
SELECT id FROM icebergLocal('iceberg_uuid/iceberg_uuid_test_part') WHERE uuid_bucket IN ('12345678-1234-5678-1234-567812345678', '80000000-0000-0000-0000-000000000000') ORDER BY id;
" 2>&1
