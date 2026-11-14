#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A bare NULL literal is Nullable(Nothing). It must reach a COLUMNAR_V1 function that handles NULL
# itself as a NULL row of the declared type. array_of_len_col returns [] for a NULL row and
# [byte_len(s)] otherwise.

MODULE="columnar_abi_${CLICKHOUSE_DATABASE}"
ARRAY_OF_LEN="wasm_array_of_len_${CLICKHOUSE_DATABASE}"
BYTES_EQUAL="wasm_bytes_equal_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"

${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/columnar_abi.wasm"

${CLICKHOUSE_CLIENT} --query "
    CREATE OR REPLACE FUNCTION ${ARRAY_OF_LEN}
    LANGUAGE WASM ABI COLUMNAR_V1 FROM '${MODULE}' :: 'array_of_len_col'
    ARGUMENTS (s Nullable(String)) RETURNS Array(UInt64) DETERMINISTIC"

${CLICKHOUSE_CLIENT} --query "SELECT ${ARRAY_OF_LEN}(NULL), ${ARRAY_OF_LEN}('')"

# The second parameter is a plain String, but the first one turns NULL handling off.
${CLICKHOUSE_CLIENT} --query "
    CREATE OR REPLACE FUNCTION ${BYTES_EQUAL}
    LANGUAGE WASM ABI COLUMNAR_V1 FROM '${MODULE}' :: 'bytes_equal_col'
    ARGUMENTS (a Nullable(String), b String) RETURNS UInt8 DETERMINISTIC"

${CLICKHOUSE_CLIENT} --query "SELECT ${BYTES_EQUAL}('abc', NULL), ${BYTES_EQUAL}('abc', 'abc')"

${CLICKHOUSE_CLIENT} --query "DROP FUNCTION ${ARRAY_OF_LEN}"
${CLICKHOUSE_CLIENT} --query "DROP FUNCTION ${BYTES_EQUAL}"
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
