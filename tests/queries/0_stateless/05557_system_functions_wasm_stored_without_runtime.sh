#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: the WebAssembly UDF engine is not built in fasttest.
# Tag no-msan: the wasmtime engine behind WASM UDFs is not instrumented for MSan.
#
# Coverage for the "stored definition without a runtime registration" branch of
# StorageSystemFunctions::fillData (src/Storages/System/StorageSystemFunctions.cpp).
# Documented behaviour, from the comment on that branch: a stored definition without a
# runtime registration (e.g. after a restart on a build without a WebAssembly engine or
# with the feature turned off) must stay visible, so that the leftover can be discovered
# and removed with DROP FUNCTION.
#
# Both runs below share one --path, i.e. one on-disk SQL objects storage:
#   run 1 is started with the WASM config, so the function is in the runtime registry and
#         system.functions reports the full signature it was declared with;
#   run 2 is started without that config, so allow_experimental_webassembly_udf is off, no
#         module manager is created and the runtime registry stays empty. Only the stored
#         CREATE query is left; the row must still be listed with origin WasmUserDefined,
#         empty signature columns and the original create_query, and DROP FUNCTION must
#         remove it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WASM_CONFIG="$CUR_DIR/../../config/config.d/wasm_udf.xml"
DATA_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}.d"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"

# The engine is enabled here, so the two-argument declaration is kept by the runtime
# registry and reported as a single syntax string.
${CLICKHOUSE_LOCAL} --path "${DATA_DIR}" --config-file "${WASM_CONFIG}" --queries-file /dev/stdin <<EOF
INSERT INTO system.webassembly_modules (name, code)
SELECT 'sf_leftover_module', raw_blob FROM file('${CUR_DIR}/wasm/identity_int.wasm', RawBLOB);

CREATE FUNCTION sf_leftover LANGUAGE WASM ABI BUFFERED_V1
    FROM 'sf_leftover_module' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32, y Int32) RETURNS Int32;

SELECT 'registered', origin, syntax, arguments != '', returned_value
FROM system.functions WHERE name = 'sf_leftover';
EOF

# Without the WASM config the stored definition is all that is left of the function.
${CLICKHOUSE_LOCAL} --path "${DATA_DIR}" --queries-file /dev/stdin <<'EOF'
-- Exactly one row, and it comes from the stored CREATE query: the WASM origin is kept,
-- the signature columns are empty and create_query is the declaration as written.
SELECT 'leftover', count(), any(origin), any(syntax = ''), any(arguments = ''),
       any(returned_value = ''),
       any(create_query LIKE '%LANGUAGE WASM%ARGUMENTS (x Int32, y Int32) RETURNS Int32%')
FROM system.functions WHERE name = 'sf_leftover';

DROP FUNCTION sf_leftover;
SELECT 'rows_after_drop', count() FROM system.functions WHERE name = 'sf_leftover';
EOF

rm -rf "${DATA_DIR}"
