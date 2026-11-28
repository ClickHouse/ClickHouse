#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PARQUET_FILE="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_variant_same_written_type.parquet"
rm -f "${PARQUET_FILE}"

function round_trip()
{
    local structure="$1"
    local values="$2"
    echo "--- ${structure}"
    ${CLICKHOUSE_CLIENT} --allow_suspicious_variant_types 1 --query "
        INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, '${structure}') ${values}
        SETTINGS engine_file_truncate_on_insert = 1;
        SELECT v, variantType(v) FROM file('${PARQUET_FILE}', Parquet, '${structure}') ORDER BY isNull(v), toString(v);
    "
}

round_trip 'v Variant(UInt32, Int64)' "SELECT 4294967295::UInt32 UNION ALL SELECT -5::Int64 UNION ALL SELECT NULL"
round_trip 'v Variant(UInt8, Int16, String)' "SELECT 7::UInt8 UNION ALL SELECT 1000::Int16 UNION ALL SELECT 'x'"
round_trip 'v Variant(String, FixedString(2))' "SELECT 'ab'::FixedString(2) UNION ALL SELECT 'xyz'"
round_trip 'v Variant(Date, Date32)' "SELECT '2024-01-01'::Date UNION ALL SELECT '1950-01-01'::Date32"

function no_member_of_written_type()
{
    local structure="$1"
    local value="$2"
    echo "--- No member is of the type that the members are read as: ${structure}"
    ${CLICKHOUSE_CLIENT} --allow_suspicious_variant_types 1 --query "
        INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, '${structure}') SELECT ${value}
        SETTINGS engine_file_truncate_on_insert = 1
    " 2>&1 | grep -o -m1 "none of these members is of this type"
}

no_member_of_written_type 'v Variant(UInt32, UInt64)' "3::UInt32"
no_member_of_written_type 'v Variant(DateTime64(3), DateTime64(5))' "toDateTime64('2024-01-01 00:00:00.12345', 5)"
no_member_of_written_type 'v Variant(Array(UInt32), Array(Int64))' "[4294967295]::Array(UInt32)"
no_member_of_written_type 'v Variant(Array(Int64), Array(String))' "['x']::Array(String)"
no_member_of_written_type 'v Variant(Array(Bool), Array(UInt8))' "[200]::Array(UInt8)"
no_member_of_written_type 'v Variant(Array(Date), Array(DateTime))' "['2149-06-06'::Date]"
no_member_of_written_type 'v Variant(Array(Decimal32(0)), Array(Decimal32(9)))' "[1000::Decimal32(0)]"
no_member_of_written_type "v Variant(Array(Enum8(''a'' = 1)), Array(Int8))" "['a']::Array(Enum8('a' = 1))"

rm -f "${PARQUET_FILE}"
