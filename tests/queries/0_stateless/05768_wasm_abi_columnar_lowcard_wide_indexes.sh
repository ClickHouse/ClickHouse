#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A COLUMNAR_V1 result declared Array(LowCardinality(UInt64)) whose decoded dictionary holds over
# 256 entries has UInt16 indexes; the result check must accept it. array_of_len_col returns
# [byte_len(s)] per row, so 256 rows of lengths 1..256 give 256 distinct values plus the default.

MODULE="columnar_abi_${CLICKHOUSE_DATABASE}"
FUNCTION="wasm_lowcard_wide_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"

${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/columnar_abi.wasm"

${CLICKHOUSE_CLIENT} --query "
    CREATE OR REPLACE FUNCTION ${FUNCTION}
    LANGUAGE WASM ABI COLUMNAR_V1 FROM '${MODULE}' :: 'array_of_len_col'
    ARGUMENTS (s String) RETURNS Array(LowCardinality(UInt64))"

${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(arraySum(${FUNCTION}(repeat('x', number + 1)))), any(toTypeName(${FUNCTION}('')))
    FROM numbers(256)
    SETTINGS webassembly_udf_max_input_block_size = 256"

${CLICKHOUSE_CLIENT} --query "DROP FUNCTION ${FUNCTION}"
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
