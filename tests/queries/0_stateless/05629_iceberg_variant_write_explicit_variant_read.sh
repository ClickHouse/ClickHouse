#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet and Iceberg writes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PARQUET_FILE="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_explicit_variant_read.parquet"
SORTED_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_explicit_variant_read_sorted"
rm -rf "${PARQUET_FILE}" "${SORTED_TABLE_PATH}"

function round_trip()
{
    local structure="$1"
    local values="$2"
    echo "--- ${structure}"
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, '${structure}') ${values}
        SETTINGS engine_file_truncate_on_insert = 1;
        SELECT v, toTypeName(v) FROM file('${PARQUET_FILE}', Parquet, '${structure}') ORDER BY isNull(v), toString(v);
    "
}

round_trip 'v Variant(UInt8, String)' "SELECT 200::UInt8 UNION ALL SELECT 'x' UNION ALL SELECT NULL"
round_trip 'v Variant(Date, String)' "SELECT '2024-01-02'::Date"
round_trip 'v Variant(DateTime, String)' "SELECT '2024-01-02 03:04:05'::DateTime"
round_trip 'v Variant(Decimal(5, 2), String)' "SELECT 123.45::Decimal(5, 2)"
round_trip 'v Variant(Array(Int64), String)' "SELECT [1, 2]::Array(Int64)"
round_trip 'v Variant(Array(Nullable(UInt8)), String)' "SELECT [1, NULL]::Array(Nullable(UInt8))"
round_trip 'v Array(Variant(UInt16, String))' "SELECT [1::UInt16, 'x', NULL]::Array(Variant(UInt16, String))"

function write_fails()
{
    ${CLICKHOUSE_CLIENT} --allow_suspicious_variant_types 1 --query "
        INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, '$1') SELECT $2
        SETTINGS engine_file_truncate_on_insert = 1
    " 2>&1 | grep -o -m1 "$3"
}

echo "--- Members that cannot be read back"
write_fails 'v Variant(Map(String, Int64), String)' "map('k', 1)::Map(String, Int64)" 'cannot be read back'
write_fails 'v Variant(Tuple(a UInt8, b String), Int64)' "tuple(1, 'x')::Tuple(a UInt8, b String)" 'cannot be read back'
write_fails 'v Variant(Array(Array(Int64)), String)' "'x'" 'cannot be read back'

echo "--- A value that does not fit the member is not converted silently"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'v Dynamic') SELECT 300::Int16
    SETTINGS engine_file_truncate_on_insert = 1;
    SELECT v FROM file('${PARQUET_FILE}', Parquet, 'v Variant(UInt8, String)');
" 2>&1 | grep -o -m1 "CANNOT_CONVERT_TYPE"

echo "--- Time64 out of the range of a time of day"
write_fails 'v Dynamic' "v FROM format(RowBinary, 'v Time64(0)', unhex('0000000000000080'))" 'a variant time is a time of day'

echo "--- Nesting depth counts the enclosing containers"
for dots in 62 63; do
    ${CLICKHOUSE_CLIENT} --max_parser_depth=64 --query "
        INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'v Dynamic')
        SELECT [concat('{\"', repeat('a.', ${dots}), 'b\": 1}')::JSON]::Dynamic
        SETTINGS engine_file_truncate_on_insert = 1;
        SELECT dynamicType(v) FROM file('${PARQUET_FILE}', Parquet);
    " 2>&1 | grep -o -m1 -E "^Array\(JSON\)$|TOO_DEEP_RECURSION"
done

echo "--- Variant sort sources"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_explicit_variant_read_sorted (id Int32, d Dynamic)
        ENGINE = IcebergLocal('${SORTED_TABLE_PATH}') ORDER BY icebergTruncate(3, d) SETTINGS iceberg_format_version = 3;
" 2>&1 | grep -o -m1 "the Iceberg type \`variant\` cannot be a sort source"
find "${SORTED_TABLE_PATH}" -name '*.metadata.json' 2>/dev/null | wc -l

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_explicit_variant_read_sorted"
rm -rf "${PARQUET_FILE}" "${SORTED_TABLE_PATH}"
