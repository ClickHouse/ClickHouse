#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SKIP and SKIP REGEXP drop the same paths in text and in binary input.
function check()
{
    # Binary JSON is read back only as the type it is written with, so typed paths are written as typed.
    local type=$1 json=$2 settings=$3 written_type=${4:-JSON}
    echo "$type${settings:+ $settings}"
    $CLICKHOUSE_LOCAL ${settings:+"$settings"} -q "SELECT CAST('$json' AS $type)"
    $CLICKHOUSE_LOCAL -q "SELECT '$json'::$written_type FORMAT RowBinary" \
        | $CLICKHOUSE_LOCAL ${settings:+"$settings"} --input-format RowBinary --structure "j $type" -q "SELECT * FROM table"
}

echo "a skipped path, not a path with the same prefix"
check "JSON(SKIP a, SKIP b)" '{"a" : 1, "ab" : 2, "b" : 3, "t" : 4}'
check "JSON(SKIP s)" '{"s" : {"x" : 4}, "t" : 5}'

echo "a path inside a skipped one, with another skipped path sorted between them"
check "JSON(SKIP a, SKIP \`a-b\`)" '{"a.x" : 1, "a-b" : 2, "c" : 3}'

echo "a regular expression that matches an object skips the paths inside it"
check "JSON(SKIP REGEXP '^a\$')" '{"a" : {"b" : 1}, "x" : 2}'
$CLICKHOUSE_LOCAL -q "SELECT * FROM values('j JSON(SKIP REGEXP \'^a\$\')', CAST('{\"a\" : {\"b\" : 1}, \"x\" : 2}' AS JSON))"

echo "a typed path is never skipped"
check "JSON(ab UInt32, SKIP REGEXP '^a')" '{"ab" : 5, "ac" : 6}' "--type_json_use_partial_match_to_skip_paths_by_regexp=1" "JSON(ab UInt32)"
$CLICKHOUSE_LOCAL --type_json_use_partial_match_to_skip_paths_by_regexp=1 -q "SELECT * FROM values('j JSON(ab UInt32, SKIP REGEXP \'^a\')', CAST('{\"ab\" : 5, \"ac\" : 6}' AS JSON))"
check "JSON(a.b UInt32, SKIP REGEXP '^a\$')" '{"a" : {"b" : 1, "c" : 2}, "x" : 3}' "" "JSON(a.b UInt32)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM values('j JSON(a.b UInt32, SKIP REGEXP \'^a\$\')', CAST('{\"a\" : {\"b\" : 1, \"c\" : 2}, \"x\" : 3}' AS JSON))"

echo "a typed path is never skipped by a conversion between JSON types"
$CLICKHOUSE_LOCAL --type_json_use_partial_match_to_skip_paths_by_regexp=1 -q "
SELECT CAST('{\"ab\" : 5, \"ac\" : 6}'::JSON AS JSON(ab UInt32, SKIP REGEXP '^a'));
SELECT CAST('{\"ab\" : 5, \"ac\" : 6}'::JSON(ab UInt32) AS JSON(ab UInt32, SKIP REGEXP '^a'));
SELECT CAST('{\"ab\" : 5, \"ac\" : 6}'::JSON(max_dynamic_paths=0) AS JSON(ab UInt32, SKIP REGEXP '^a'));"

echo "regular expressions match partially or fully by the setting"
check "JSON(SKIP REGEXP 'b')" '{"ab" : 1, "c" : 2}' "--type_json_use_partial_match_to_skip_paths_by_regexp=1"
check "JSON(SKIP REGEXP 'b')" '{"ab" : 1, "c" : 2}' "--type_json_use_partial_match_to_skip_paths_by_regexp=0"

echo "the limit on the number of paths of an object in binary input"
$CLICKHOUSE_LOCAL -q "SELECT '{\"a\" : 1, \"b\" : 2, \"c\" : 3}'::JSON FORMAT RowBinary" \
    | $CLICKHOUSE_LOCAL --format_binary_max_object_size=2 --input-format RowBinary --structure "j JSON" -q "SELECT * FROM table" 2>&1 | grep -o -m1 "TOO_LARGE_ARRAY_SIZE"

echo "a typed path inside a skipped path is not allowed, a path with the same prefix is"
$CLICKHOUSE_LOCAL -q "SELECT '{\"ab\" : 1}'::JSON(ab UInt32, SKIP a)"
$CLICKHOUSE_LOCAL -q "SELECT '{}'::JSON(\`a.b\` UInt32, SKIP a)" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "SELECT '{}'::JSON(a UInt32, SKIP a)" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
