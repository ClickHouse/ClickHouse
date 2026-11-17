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

round_trip 'v Variant(UInt32, Int64)' "SELECT 1::UInt32 UNION ALL SELECT -5::Int64 UNION ALL SELECT NULL"
round_trip 'v Variant(UInt8, Int16, String)' "SELECT 7::UInt8 UNION ALL SELECT 1000::Int16 UNION ALL SELECT 'x'"
round_trip 'v Variant(UInt32, UInt64)' "SELECT 3::UInt32 UNION ALL SELECT 4::UInt64"
round_trip 'v Variant(Array(Int64), Array(String))' "SELECT [1, 2]::Array(Int64)"

rm -f "${PARQUET_FILE}"
