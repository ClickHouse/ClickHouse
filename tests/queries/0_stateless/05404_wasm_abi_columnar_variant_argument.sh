#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A declared Variant argument reaches a COLUMNAR_V1 guest as one COL_VARIANT column holding every
# alternative and NULL, instead of being dispatched by the framework per alternative. The guest
# returns NULL (an error on the host) for any other descriptor and otherwise prints each row as
# "<discriminator>:<hex of the value>".

MODULE="columnar_wide_abi_${CLICKHOUSE_DATABASE}"
FUNCTION="wasm_variant_hex_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"

${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/columnar_wide_abi.wasm"

${CLICKHOUSE_CLIENT} --query "
    CREATE OR REPLACE FUNCTION ${FUNCTION}
    LANGUAGE WASM ABI COLUMNAR_V1 FROM '${MODULE}' :: 'hex_variant_col'
    ARGUMENTS (v Variant(String, UInt64)) RETURNS String DETERMINISTIC"

# Alternatives are sorted by name: String is 0, UInt64 is 1.
${CLICKHOUSE_CLIENT} --query "
    SELECT number, v, ${FUNCTION}(v)
    FROM
    (
        SELECT number, multiIf(number = 0, 'ab'::Variant(String, UInt64), number = 1, 258::UInt64::Variant(String, UInt64),
            number = 2, NULL, number = 3, ''::Variant(String, UInt64), 1::UInt64::Variant(String, UInt64)) AS v
        FROM numbers(5)
    )
    ORDER BY number"

${CLICKHOUSE_CLIENT} --query "DROP FUNCTION ${FUNCTION}"
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
