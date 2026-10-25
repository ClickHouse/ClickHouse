#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# no-parallel: the `wasm_module_manager_pause_after_compile` failpoint is server-global, so a
#   concurrent test loading any WebAssembly module would consume it.

# A WebAssembly module is compiled without holding the lock of the module manager, so deleting a
# module does not wait for a concurrent compilation. A compilation that finishes after its module was
# deleted fails with `RESOURCE_NOT_FOUND` and does not bring the module back.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP=wasm_module_manager_pause_after_compile
MODULE="compile_unlocked_${CLICKHOUSE_DATABASE}"
FUNCTION="wasm_compile_unlocked_${CLICKHOUSE_DATABASE}"
CREATE_OUTPUT="${CLICKHOUSE_TMP}/${MODULE}.create"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FP}"
    wait
    ${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${FUNCTION}"
    ${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
    rm -f "${CREATE_OUTPUT}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/exceptions.wasm"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${FP}"

# The first use of the module compiles it and parks at the failpoint with the compiled module not yet
# published.
${CLICKHOUSE_CLIENT} --query "
    CREATE FUNCTION ${FUNCTION} LANGUAGE WASM ABI ROW_DIRECT FROM '${MODULE}' :: 'catch_exception'
    ARGUMENTS (UInt32) RETURNS UInt32" > "${CREATE_OUTPUT}" 2>&1 &

if ! timeout 60 ${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
then
    echo "The compilation did not reach the failpoint"
    exit 1
fi

# Completes while the compilation is parked; it would block until the timeout if the compilation held
# the lock of the module manager.
timeout 60 ${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'" && echo "deleted"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FP}"
wait

grep -o 'RESOURCE_NOT_FOUND' "${CREATE_OUTPUT}" | head -1
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.webassembly_modules WHERE name = '${MODULE}'"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.functions WHERE name = '${FUNCTION}'"
