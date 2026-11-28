#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet and Iceberg writes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PARQUET_FILE="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_variant_round_trip.parquet"
TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_variant_round_trip"
PARTITIONED_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_variant_round_trip_partitioned"
rm -rf "${PARQUET_FILE}" "${TABLE_PATH}" "${PARTITIONED_TABLE_PATH}"

echo "--- Literal dots in JSON keys"
${CLICKHOUSE_CLIENT} --json_type_escape_dots_in_keys=1 --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'v Dynamic')
    SELECT ('{\"a.b\": 1, \"a\": {\"b\": 2}, \"c\": {\"d.e\": {\"f\": 3}}}'::JSON)::Dynamic
    SETTINGS engine_file_truncate_on_insert = 1;
    SELECT v, arraySort(JSONAllPaths(v::JSON)) FROM file('${PARQUET_FILE}', Parquet);
"

echo "--- Unlimited max_parser_depth"
${CLICKHOUSE_CLIENT} --max_parser_depth=0 --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'v Dynamic') SELECT ('{\"a\": {\"b\": 1}}'::JSON)::Dynamic
    SETTINGS engine_file_truncate_on_insert = 1;
"
${CLICKHOUSE_CLIENT} --query "SELECT v FROM file('${PARQUET_FILE}', Parquet)"

echo "--- Deep dotted path with a raised max_parser_depth"
${CLICKHOUSE_CLIENT} --max_parser_depth=1000000 --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'v Dynamic')
    SELECT (concat('{\"', repeat('a.', 500000), 'x\": 1}')::JSON)::Dynamic
    SETTINGS engine_file_truncate_on_insert = 1
" 2>&1 | grep -o -m1 "TOO_DEEP_RECURSION"
${CLICKHOUSE_CLIENT} --query "SELECT 'alive'"

echo "--- Explicit Variant schema"
STRUCTURE='v Variant(Int64, String), a Array(Variant(Int64, String)), t Tuple(x Variant(Int64, String)), m Map(String, Variant(Int64, String))'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, '${STRUCTURE}')
    SELECT 42::Int64, [1::Int64, 'x'], tuple('y'), map('k', 7::Int64)
    UNION ALL SELECT 'z', [], tuple(NULL), map()
    UNION ALL SELECT NULL, [NULL], tuple(3::Int64), map('k', NULL)
    SETTINGS engine_file_truncate_on_insert = 1;
    SELECT v, variantType(v), a, t, m FROM file('${PARQUET_FILE}', Parquet, '${STRUCTURE}') ORDER BY toString(v);
"

echo "--- Variant map keys are required"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'm Map(Dynamic, String)')
    SELECT map(1::Dynamic, 'x', 'k'::Dynamic, 'y')
    SETTINGS engine_file_truncate_on_insert = 1;
    SELECT arrayMap(c -> (c.path, c.max_definition_level, c.max_repetition_level), columns)
    FROM file('${PARQUET_FILE}', ParquetMetadata);
    SELECT m FROM file('${PARQUET_FILE}', Parquet);
"

echo "--- Widening a variant column"
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t_variant_round_trip (v Variant(Int64, String))
        ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_format_version = 3;
    INSERT INTO t_variant_round_trip VALUES (1), ('x');
    ALTER TABLE t_variant_round_trip MODIFY COLUMN v Variant(Int64, String, Float64);
    INSERT INTO t_variant_round_trip VALUES (2.5);
    SELECT v, dynamicType(v) FROM t_variant_round_trip ORDER BY toString(v);
"

echo "--- Variant partition sources"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --allow_suspicious_types_in_order_by=1 --query "
    CREATE TABLE t_variant_round_trip_partitioned (id Int32, d Dynamic)
        ENGINE = IcebergLocal('${PARTITIONED_TABLE_PATH}') PARTITION BY d SETTINGS iceberg_format_version = 3;
" 2>&1 | grep -o -m1 "the Iceberg type \`variant\` cannot be a partition source"
find "${PARTITIONED_TABLE_PATH}" -name '*.metadata.json' 2>/dev/null | wc -l

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_variant_round_trip;
    DROP TABLE IF EXISTS t_variant_round_trip_partitioned;
"
rm -rf "${PARQUET_FILE}" "${TABLE_PATH}" "${PARTITIONED_TABLE_PATH}"
