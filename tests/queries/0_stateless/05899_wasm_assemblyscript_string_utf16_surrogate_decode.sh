#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: the WebAssembly UDF engine (wasmtime) is not built in fasttest.
# Tag no-msan: the wasmtime engine behind WASM UDFs is not instrumented for MSan.
#
# `appendUTF16LEAsUTF8` (src/Functions/UserDefined/UserDefinedWebAssemblyScriptAbi.cpp) decodes the
# UTF-16LE bytes of the AssemblyScript `String` a WASM UDF returned. Documented behaviour,
# docs/reference/functions/regular-functions/wasm_udf.mdx: "`String` -- maps to AssemblyScript
# `string` (UTF-16 in WASM memory). ClickHouse handles the UTF-8 <-> UTF-16 conversion
# automatically."
#
# Only ASCII and two-byte returns are asserted today (04210 returns `Hello, Привет!`), so the two
# widest forms of the decoder are unchecked: a code point in U+0800..U+FFFF has to come back as
# three UTF-8 bytes, and a code point above U+FFFF is stored in WASM memory as a surrogate pair
# (U+1F980 = D83E DD80) that has to be recombined into the one code point and re-encoded as four
# bytes, with the low surrogate consumed by its high surrogate rather than decoded a second time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AS_MOD="as_utf16_${CLICKHOUSE_DATABASE}"
GREET="wasm_as_utf16_greet_${CLICKHOUSE_DATABASE}"
REPEAT="wasm_as_utf16_repeat_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${GREET};
DROP FUNCTION IF EXISTS ${REPEAT};
DELETE FROM system.webassembly_modules WHERE name = '${AS_MOD}';
EOF

cat "${CUR_DIR}/wasm/as_example.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${AS_MOD}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} << EOF
CREATE OR REPLACE FUNCTION ${GREET}
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM '${AS_MOD}' :: 'greet'
    ARGUMENTS (name String) RETURNS String;

CREATE OR REPLACE FUNCTION ${REPEAT}
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM '${AS_MOD}' :: 'str_repeat'
    ARGUMENTS (s String, n UInt32) RETURNS String;

SELECT 'three-byte code point: returned string, its UTF-8 bytes, its code points';
SELECT ${GREET}('→') AS s, hex(s), lengthUTF8(s);

SELECT 'one code point above U+FFFF: the surrogate pair becomes four UTF-8 bytes';
SELECT ${GREET}('🦀') AS s, hex(s), lengthUTF8(s);

SELECT 'all four UTF-8 widths in one returned string';
SELECT ${GREET}('aé→🦀') AS s, hex(s), lengthUTF8(s);

SELECT 'three adjacent pairs: each low surrogate is consumed, not decoded on its own';
SELECT ${REPEAT}('🦀', 3::UInt32) AS s, hex(s), lengthUTF8(s), length(s);

SELECT 'lossless at every UTF-8 width boundary: input bytes, bytes of the returned string';
SELECT hex(b) AS src_hex, hex(${GREET}(b)) AS out_hex
FROM (SELECT arrayJoin([unhex('7F'), unhex('C280'), unhex('DFBF'), unhex('E0A080'),
                        unhex('EFBFBD'), unhex('F0908080'), unhex('F48FBFBF')]) AS b)
ORDER BY src_hex;
EOF

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${GREET};
DROP FUNCTION IF EXISTS ${REPEAT};
DELETE FROM system.webassembly_modules WHERE name = '${AS_MOD}';
EOF
