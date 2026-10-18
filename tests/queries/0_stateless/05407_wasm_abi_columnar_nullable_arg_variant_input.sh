#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A declared Nullable parameter means the COLUMNAR_V1 guest handles NULL rows itself. The default
# Variant adaptor handles NULL rows on its own, so it must not be used for such a function: it would
# drop the NULL row before the guest sees it and make the result Nullable. A Variant argument is
# rejected instead, while the equivalent Nullable argument reaches the guest. str_byte_sum_col
# answers UINT64_MAX (18446744073709551615) for a row its null map marks.

MODULE="columnar_abi_${CLICKHOUSE_DATABASE}"
FUNCTION="wasm_str_byte_sum_nullable_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"

${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/columnar_abi.wasm"

${CLICKHOUSE_CLIENT} --query "
    CREATE OR REPLACE FUNCTION ${FUNCTION}
    LANGUAGE WASM ABI COLUMNAR_V1 FROM '${MODULE}' :: 'str_byte_sum_col'
    ARGUMENTS (s Nullable(String)) RETURNS UInt64 DETERMINISTIC"

${CLICKHOUSE_CLIENT} --query "
    SELECT number, ${FUNCTION}(s), toTypeName(${FUNCTION}(s))
    FROM (SELECT number, CAST(if(number = 0, NULL, 'ab'), 'Nullable(String)') AS s FROM numbers(2))
    ORDER BY number"

${CLICKHOUSE_CLIENT} --query "
    SELECT number, ${FUNCTION}(s)
    FROM (SELECT number, CAST(if(number = 0, NULL, 'ab'), 'Variant(String)') AS s FROM numbers(2))
    ORDER BY number" 2>&1 | grep -o 'ILLEGAL_TYPE_OF_ARGUMENT' | head -1

${CLICKHOUSE_CLIENT} --query "DROP FUNCTION ${FUNCTION}"
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
