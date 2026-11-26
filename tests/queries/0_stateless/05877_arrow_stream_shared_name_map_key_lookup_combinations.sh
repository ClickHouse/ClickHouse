#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `ArrowStream` format needs Arrow, which is not built in the fast test.

# With case-insensitive matching, a map key lookup (`t.key_...`) is resolved against every type requested for a
# name that several columns share:
# - including a type that equals the merge of the types requested before it, in any order of the header;
# - with every combination of the types of several shared names, up to a limit: each shared element of a tuple
#   key multiplies the number of combinations, and resolving the lookup must not take exponential time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$dir"
mkdir -p "$dir"
cd "$dir" || exit 1

$CLICKHOUSE_LOCAL <<'EOF'
SET input_format_arrow_case_insensitive_column_matching = 1;

WITH repeat(char(0), 16) AS bytes
SELECT CAST(map(tuple(bytes, bytes), toUInt8(7)) AS Map(Tuple(a String, b String), UInt8)) AS mm
INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;

SELECT 'a type equal to the merge of the other two';
SELECT `mm.key_(0,0)`, mm FROM file('data.arrows', 'ArrowStream', 'MM Map(Tuple(a Int128, b String), UInt8), Mm Map(Tuple(a String, b Int128), UInt8), mm Map(Tuple(a Int128, b Int128), UInt8), `mm.key_(0,0)` UInt8') WHERE NOT ignore(*);
SELECT `mm.key_(0,0)`, mm FROM file('data.arrows', 'ArrowStream', 'MM Map(Tuple(a Int128, b String), UInt8), mm Map(Tuple(a Int128, b Int128), UInt8), Mm Map(Tuple(a String, b Int128), UInt8), `mm.key_(0,0)` UInt8') WHERE NOT ignore(*);
SELECT `mm.key_(0,0)`, mm FROM file('data.arrows', 'ArrowStream', 'mm Map(Tuple(a Int128, b Int128), UInt8), MM Map(Tuple(a Int128, b String), UInt8), Mm Map(Tuple(a String, b Int128), UInt8), `mm.key_(0,0)` UInt8') WHERE NOT ignore(*);
SELECT `mm.key_(0,0)`, mm FROM file('data.arrows', 'ArrowStream', '`mm.key_(0,0)` UInt8, Mm Map(Tuple(a String, b Int128), UInt8), MM Map(Tuple(a Int128, b String), UInt8), mm Map(Tuple(a Int128, b Int128), UInt8)') WHERE NOT ignore(*);

SELECT 'a type requested twice';
SELECT `mm.key_(0,0)`, mm FROM file('data.arrows', 'ArrowStream', 'MM Map(Tuple(a Int128, b String), UInt8), mM Map(Tuple(a Int128, b String), UInt8), Mm Map(Tuple(a String, b Int128), UInt8), mm Map(Tuple(a Int128, b Int128), UInt8), `mm.key_(0,0)` UInt8') WHERE NOT ignore(*);

SELECT 'no type resolves the key';
SELECT `mm.key_(0,0)` FROM file('data.arrows', 'ArrowStream', 'MM Map(Tuple(a Int128, b String), UInt8), Mm Map(Tuple(a String, b Int128), UInt8), `mm.key_(0,0)` UInt8') WHERE NOT ignore(*);
SELECT `mm.key_(0,0)` FROM file('data.arrows', 'ArrowStream', 'Mm Map(Tuple(a String, b Int128), UInt8), MM Map(Tuple(a Int128, b String), UInt8), `mm.key_(0,0)` UInt8') WHERE NOT ignore(*);
EOF

# A map with a tuple key of `$1` elements, each of them requested as `t.keys.fN Array(String)` and as
# `T.KEYS.FN Array(Int128)` (in this order, or in the opposite one with `$2` = 1), and a lookup of the key that
# only the `Int128` representation of every element resolves.
function lookup_with_shared_key_elements()
{
    local count=$1
    local reversed=$2
    local types=""
    local values=""
    local key=""
    local structure=""
    for ((i = 0; i < count; ++i))
    do
        types+="${types:+, }f$i String"
        values+="${values:+, }repeat(char(0), 16)"
        key+="${key:+,}0"
        if [ "$reversed" = 1 ]
        then
            structure+="\`T.KEYS.F$i\` Array(Int128), \`t.keys.f$i\` Array(String), "
        else
            structure+="\`t.keys.f$i\` Array(String), \`T.KEYS.F$i\` Array(Int128), "
        fi
    done
    structure+="\`t.key_($key)\` UInt8"

    $CLICKHOUSE_LOCAL --input_format_arrow_case_insensitive_column_matching 1 --query "
        SELECT CAST(map(tuple($values), toUInt8(7)) AS Map(Tuple($types), UInt8)) AS t
        INTO OUTFILE 'wide.arrows' TRUNCATE FORMAT ArrowStream;
        SELECT \`t.key_($key)\` FROM file('wide.arrows', 'ArrowStream', '$structure') WHERE NOT ignore(*);
    " 2>&1 | grep -o -m1 -E '^[0-9]+$|BAD_ARGUMENTS'
}

echo 'several shared elements of a tuple key'
lookup_with_shared_key_elements 1 0
lookup_with_shared_key_elements 1 1
lookup_with_shared_key_elements 4 0
lookup_with_shared_key_elements 4 1
lookup_with_shared_key_elements 8 0
lookup_with_shared_key_elements 8 1

echo 'too many combinations'
lookup_with_shared_key_elements 9 0
lookup_with_shared_key_elements 30 0
lookup_with_shared_key_elements 30 1

cd "$CLICKHOUSE_TMP" || exit 1
rm -rf "$dir"
