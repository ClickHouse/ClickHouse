#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-parallel
# Tag no-fasttest: the WebAssembly runtime is not part of the fasttest build
# Tag no-msan: wasmtime compiles the guest at run time, which MemorySanitizer cannot track
# Tag no-parallel: the test adds and removes a module in system.webassembly_modules and creates
#     server-global UDFs, both system-global entities

# Decoding of the AssemblyScript strings that `env.abort(message, fileName, line, column)` hands to
# the host: src/Interpreters/WebAssembly/HostApi.cpp, decodeAssemblyScriptString. The expected
# behaviour is the contract of the doc comment above that function: the message is a UTF-16 payload
# whose byte length sits in a u32 runtime header at -4 from the pointer, non-ASCII code units become
# '?', and a header that cannot describe a string decodes to an empty message.
#
# The decoded text reaches the user only as the payload of the WASM_ERROR message, so the test pulls
# exactly that payload out of the message and asserts it. It asserts no other part of the message:
# not its wording, not the code/line/column, not the two cases that fail before any decoding (those
# are asserted by their error code name only). A decoder that is bypassed, that returns an empty
# string for pointer 44, or that keeps the non-ASCII unit, changes the asserted value.
#
# `abort` is linked for every WASM ABI (HostApi.cpp, getHostFunction), so an ordinary ROW_DIRECT
# module reaches the decoder with a pointer chosen by the SQL argument. The module is assembled by
# hand and inlined, as in 03206_wasm_module_manage; its source is
#
#   (module
#     (import "env" "abort" (func $abort (param i32 i32 i32 i32)))
#     (memory (export "memory") 1)
#     (data (i32.const 16) "...")   ;; the string objects, see the pointer table below
#     (func (export "abort_at") (param i32) (result i32)
#       local.get 0  i32.const 0  i32.const 11  i32.const 7  call $abort  unreachable)
#     (func (export "add_self") (param i32) (result i32)
#       local.get 0  local.get 0  i32.add))
#
# The data segment starts at offset 16 and is
#   16: 00000000 00000000 03000000 00000000 00000002 00000000 06000000 41004200 c4000000 00001000
# so the argument of abort_at, which is the message pointer, selects a runtime header:
#   0   null pointer                                   -> empty message
#   1   inside the 4-byte header, no object can start  -> pointer rejected before decoding
#   20  rtSize = 0                                     -> empty message
#   28  rtSize = 3, not a whole number of UTF-16 units -> empty message
#   48  rtSize = 0x420041, likewise odd                -> empty message
#   36  rtSize = 2^25, above the 2^24 limit            -> empty message
#   44  rtSize = 6, payload 'A' 'B' U+00C4             -> decoded as AB?, non-ASCII becomes '?'
#   56  rtSize = 1 MiB, payload past the end of memory -> payload rejected before decoding
# fileName is always the null pointer, so it decodes to the empty string in every case.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP FUNCTION IF EXISTS as_abort_at;
DROP FUNCTION IF EXISTS as_add_self;
DELETE FROM system.webassembly_modules WHERE name = 'as_abort_probe';

INSERT INTO system.webassembly_modules (name, code) SELECT 'as_abort_probe', base64Decode(concat(
    'AGFzbQEAAAABDQJgBH9/f38AYAF/AX8CDQEDZW52BWFib3J0AAADAwIBAQUDAQABByADBm1l',
    'bW9yeQIACGFib3J0X2F0AAEIYWRkX3NlbGYAAgoXAg0AIABBAEELQQcQAAALBwAgACAAagsL',
    'LgEAQRALKAAAAAAAAAAAAwAAAAAAAAAAAAACAAAAAAYAAABBAEIAxAAAAAAAEAA='));

CREATE FUNCTION as_abort_at LANGUAGE WASM ABI ROW_DIRECT
    FROM 'as_abort_probe' :: 'abort_at' ARGUMENTS (ptr UInt32) RETURNS Int32;
CREATE FUNCTION as_add_self LANGUAGE WASM ABI ROW_DIRECT
    FROM 'as_abort_probe' :: 'add_self' ARGUMENTS (x UInt32) RETURNS Int32;
"

# The module itself is sound: the export that does not abort returns its value.
$CLICKHOUSE_CLIENT -q "SELECT as_add_self(21 :: UInt32)"

# Print the decoded message of the abort, in brackets so that an empty one stays visible. A query
# that fails before any string is decoded has no such payload; print its error code name instead.
decoded_message()
{
    local out
    out=$($CLICKHOUSE_CLIENT -q "$1" 2>&1)
    local payload
    payload=$(echo "$out" | sed -n 's/.*module aborted: \(.*\) (at .*/decoded=[\1]/p' | head -n 1)
    if [ -n "$payload" ]; then
        echo "$payload"
    else
        echo "$out" | grep -o -m 1 'WASM_ERROR'
    fi
}

# One query per shape of the runtime header.
for ptr in 0 1 20 28 48 36 44 56; do
    echo -n "$ptr "
    decoded_message "SELECT as_abort_at($ptr :: UInt32)"
done

# The same decoding through the row loop of a real block, not through constant folding. Row 0 of the
# block aborts first, with the null message pointer.
echo -n "block "
decoded_message "SELECT as_abort_at((number * 20) :: UInt32) FROM numbers(3)"

# Decoding a broken message leaves no damage behind: the module is still callable.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM numbers(8) WHERE as_add_self(number :: UInt32) = number * 2"

$CLICKHOUSE_CLIENT -q "
DROP FUNCTION as_abort_at;
DROP FUNCTION as_add_self;
DELETE FROM system.webassembly_modules WHERE name = 'as_abort_probe';
"
