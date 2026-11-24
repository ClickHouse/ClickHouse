#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Fast test build has neither simdjson nor rapidjson.

# Every function of src/Functions/FunctionsJSON.cpp must return for a `JSON` column what it returns for the same
# document as a `String`. The expected side runs the function on the source document where the `JSON` type keeps it
# as is, and on `c::String` where the type normalizes it (dropped nulls and empty objects, sorted keys, a dotted key
# stored as a nested path, typed paths). Every mismatching (call, column, row) is printed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TYPED="\`a.c\` Int64, d Array(Nullable(Int64)), s String, n Nullable(Int64), lc LowCardinality(String), t Tuple(x Int64, y String), m Map(String, Int64)"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE src (id UInt32, canon UInt8, s String) ENGINE = Memory;
INSERT INTO src VALUES
    (1, 1, '{\"a\":{\"b\":\"x\",\"c\":1},\"d\":[1,2],\"s\":\"hi\"}'),
    (2, 1, '{\"a\":{\"e\":{\"f\":{\"g\":7}}},\"arr\":[{\"k\":1},{\"k\":\"v\",\"w\":[1,2]}],\"d\":[-1,null,3],\"na\":[[1,2],[3]],\"x\":{\"y\":{\"z\":3}}}'),
    (3, 1, '{\"b\":true,\"big\":18446744073709551615,\"f\":1.5,\"g\":0.1,\"h\":-2.25,\"neg\":-3,\"s\":\"12\",\"str\":\"a/b \\\\\"q\\\\\" é\",\"t\":{\"x\":5,\"y\":\"z\"}}'),
    (4, 1, '{\"A\":{\"B\":1},\"S\":\"up\",\"a\":{\"b\":\"low\"},\"s\":\"lo\"}'),
    (5, 1, '{\"m\":{\"k1\":1,\"k2\":-2},\"x\":\"str\"}'),
    (6, 1, '{\"lc\":\"v\",\"x\":42}'),
    (7, 1, '{\"d\":[],\"e\":[]}'),
    (8, 0, '{\"z\":null,\"o\":{},\"n\":null,\"a\":{\"c\":2},\"b\":false}'),
    (9, 0, '{\"a.b\":1}'),
    (10, 0, '{\"a\":5,\"a.b\":1}'),
    (11, 0, '{\"p.q\":{\"r\":2}}'),
    (12, 1, '{\"k%2Eq\":1,\"k\`q\":2}'),
    (13, 1, '{\"a\":5}'),
    (14, 1, '{\"\":{\"a\":42}}'),
    (15, 1, '{}'),
    (16, 1, '{\"f\":1.25e-7,\"n\":-7,\"x\":{\"y\":[1,\"a\",null]}}'),
    (17, 0, '{\"x\":{\"y\":{\"z\":\"deep\"}},\"a\":{\"e\":{\"f\":{\"g\":\"str\"}}},\"d\":[5]}'),
    (18, 0, '{\"a\":42,\"a\":{\"b\":42}}'),
    (19, 0, '{\"a\":{\"b\":42},\"a\":42}');
"

# The expected side reads `o_<column>`: the source document of a canonical row of `t`, otherwise the column as
# `String`. Each table is filled under the settings its calls run with.
create_table()
{
    local name=$1 source=$2 settings=$3 canon=$4
    $CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${name}_0 (id UInt32, canon UInt8, s String, j_dyn JSON, j_typed JSON($TYPED),
        j_shared JSON(max_dynamic_paths = 1), j_shared0 JSON(max_dynamic_paths = 0)) ENGINE = Memory;
    INSERT INTO ${name}_0 SELECT id, canon, s, s, s, s, s FROM $source $settings;
    CREATE TABLE $name (id UInt32, canon UInt8, s String, j_dyn JSON, j_typed JSON($TYPED),
        j_shared JSON(max_dynamic_paths = 1), j_shared0 JSON(max_dynamic_paths = 0),
        o_dyn String, o_typed String, o_shared String, o_shared0 String) ENGINE = Memory;
    INSERT INTO $name SELECT id, canon, s, j_dyn, j_typed, j_shared, j_shared0,
        if(canon AND $canon, s, j_dyn::String), j_typed::String, if(canon AND $canon, s, j_shared::String),
        if(canon AND $canon, s, j_shared0::String)
        FROM ${name}_0 $settings;
    "
}

create_table t src "SETTINGS type_json_allow_duplicated_key_with_literal_and_nested_object = 1" 1
create_table te src "SETTINGS type_json_allow_duplicated_key_with_literal_and_nested_object = 1, json_type_escape_dots_in_keys = 1" 0

$CLICKHOUSE_CLIENT -q "CREATE TABLE res (tbl String, call String, col String, id UInt32, r String, e String) ENGINE = Memory"

# One call per line: <function>|<path arguments, each with a leading comma>|<result type of JSONExtract*>.
# Arguments, if any: the result types to generate, instead of all of them.
gen_calls()
{
    local plain=(JSONHas JSONLength JSONKey JSONType JSONExtractInt JSONExtractUInt JSONExtractFloat JSONExtractBool
        JSONExtractString JSONExtractRaw JSONExtractArrayRaw JSONExtractKeysAndValuesRaw JSONExtractKeys
        JSONExtractIntCaseInsensitive JSONExtractUIntCaseInsensitive JSONExtractFloatCaseInsensitive
        JSONExtractBoolCaseInsensitive JSONExtractStringCaseInsensitive JSONExtractRawCaseInsensitive
        JSONExtractArrayRawCaseInsensitive JSONExtractKeysAndValuesRawCaseInsensitive JSONExtractKeysCaseInsensitive)
    local typed=(JSONExtract JSONExtractCaseInsensitive JSONExtractKeysAndValues JSONExtractKeysAndValuesCaseInsensitive)
    local paths=("" "'a'" "'d'" "'s'" "'n'" "'lc'" "'t'" "'m'" "'arr'" "'x'" "'e'" "'f'" "'g'" "'b'" "'big'" "'neg'"
        "'str'" "'na'" "'o'" "'z'" "'A'" "'S'" "'missing'" "''" "'a.b'" "'p.q'" "'a%2Eb'" "'k%2Eq'" "'k\`q'"
        "'a', 'b'" "'a', 'c'" "'a', 'e', 'f'" "'a', 'e', 'f', 'g'" "'x', 'y'" "'x', 'y', 'z'" "'t', 'x'" "'m', 'k1'"
        "'m', 'keys'" "'d', 'size0'" "'a', 'missing'" "'', 'a'" "'A', 'B'" "'p', 'q'" "'p', 'q', 'r'" "'p.q', 'r'"
        "'arr', 'k'" "'a.b', 'c'"
        "1" "-1" "2" "10" "0" "'d', 1" "'d', -1" "'d', 3" "'d', 18446744073709551615" "'arr', 2, 'k'"
        "'arr', 2, 'w', -1" "'a', 1" "'a', -1" "'a', 'e', 'f', 1" "'a', 'e', 'f', 1, 'g'" "'na', 1, 2" "'x', 1"
        "'x', 'y', 3"
        "'D'" "'Str'" "'A', 'b'" "'a', 'B'" "'X', 'Y', 'Z'" "'M', 'K1'"
        "materialize('a')" "materialize('d'), materialize(1)" "'a', materialize('b')" "materialize('x'), 'y', materialize('z')"
        "materialize('f')" "materialize('s')" "materialize('t'), 'x'" "materialize('A')" "materialize('a.b')"
        "materialize(toNullable('s'))" "materialize(toUInt8(2))")
    local tpaths=("" "'a'" "'d'" "'s'" "'n'" "'f'" "'b'" "'big'" "'t'" "'m'" "'arr'" "'missing'" "'S'")
    local types=("Int64" "UInt8" "Float64" "Bool" "String" "Nullable(Int64)" "Nullable(String)" "LowCardinality(String)"
        "LowCardinality(Nullable(String))" "Enum8('x' = 1, 'hi' = 2)" "Array(Int64)" "Array(Nullable(String))"
        "Array(Array(Int64))" "Tuple(b String, c Int64)" "Tuple(Int64, Int64)" "Map(String, String)"
        "Map(String, Nullable(Int64))" "Decimal(10, 2)" "DateTime64(3, 'UTC')" "Dynamic" "Variant(Int64, String)" "JSON")
    [ $# -gt 0 ] && types=("$@")
    local f p ty
    printf 'isValidJSON||\n'
    for f in "${plain[@]}"; do
        for p in "${paths[@]}"; do
            printf '%s|%s|\n' "$f" "${p:+, $p}"
        done
    done
    for f in "${typed[@]}"; do
        for ty in "${types[@]}"; do
            for p in "${tpaths[@]}"; do
                printf '%s|%s|%s\n' "$f" "${p:+, $p}" "$ty"
            done
        done
    done
}

# The functions below set REPLY instead of printing, so that generating thousands of calls forks no subshell.

# The SQL string literal of $1.
sql_quote() { local s=${1//\\/\\\\}; REPLY="'${s//\'/\\\'}'"; }

# The printed form of the result $3 of the function $1 with the result type $2; a type the value does not show is printed too.
result_text()
{
    local e=$3 of=""
    case "$2" in
        Dynamic) of=dynamicType ;;
        Variant*) of=variantType ;;
        JSON) of=JSONAllPathsWithTypes ;;
    esac
    if [ -z "$of" ]; then
        REPLY="toString(tuple($e))"
    elif [[ $1 == JSONExtractKeysAndValues* ]]; then
        REPLY="toString(tuple($e, arrayMap(p -> $of(p.2), $e)))"
    else
        REPLY="toString(tuple($e, $of($e)))"
    fi
}

# Reads calls from stdin and writes one INSERT per group of calls of the same function and result type.
build_queries()
{
    local tbl=$1 settings=$2 columns=($3)
    local f args ty tyarg label c r group="" key=""
    while IFS='|' read -r f args ty; do
        if [ "$f|$ty" != "$key" ] && [ -n "$group" ]; then
            echo "INSERT INTO res SELECT '$tbl', x.1, x.2, id, x.3, x.4 FROM (SELECT id, [${group#,}] AS xs FROM $tbl) ARRAY JOIN xs AS x $settings;"
            group=""
        fi
        key="$f|$ty"
        tyarg=""
        if [ -n "$ty" ]; then sql_quote "$ty"; tyarg=", $REPLY"; fi
        sql_quote "$f(c$args$tyarg)"; label=$REPLY
        for c in "${columns[@]}"; do
            result_text "$f" "$ty" "$f($c$args$tyarg)"; r=$REPLY
            result_text "$f" "$ty" "$f(o_${c#j_}$args$tyarg)"
            group+=",($label, '$c', $r, $REPLY)"
        done
    done
    [ -n "$group" ] && echo "INSERT INTO res SELECT '$tbl', x.1, x.2, id, x.3, x.4 FROM (SELECT id, [${group#,}] AS xs FROM $tbl) ARRAY JOIN xs AS x $settings;"
}

# With escaped dots the paths of the keys change, not the conversion of a value: fewer result types are enough there.
ALL_COLUMNS="j_dyn j_typed j_shared j_shared0"
TE_TYPES=("String" "Dynamic" "JSON" "Tuple(b String, c Int64)" "Map(String, Nullable(Int64))")
echo "calls: $(gen_calls | wc -l) $(gen_calls "${TE_TYPES[@]}" | wc -l)"
gen_calls | build_queries t "" "$ALL_COLUMNS" | $CLICKHOUSE_CLIENT
gen_calls "${TE_TYPES[@]}" | build_queries te "SETTINGS json_type_escape_dots_in_keys = 1" "$ALL_COLUMNS" | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT -q "
SELECT '-- the data: rows, text size, rows whose source is not what the JSON type keeps (must be 0)';
SELECT 't', count(), sum(length(o_dyn)), sum(length(o_typed)), countIf(canon AND (JSONExtractRaw(j_dyn::String) != JSONExtractRaw(s)
    OR JSONExtractRaw(j_shared::String) != JSONExtractRaw(s) OR JSONExtractRaw(j_shared0::String) != JSONExtractRaw(s))) FROM t;
SELECT 'te', count(), sum(length(o_dyn)), sum(length(o_typed)) FROM te;
"

# Presentation settings change how a result is printed, never which value is extracted.
PRESENTATION="output_format_float_precision = 2, output_format_json_quote_64bit_integers = 1, output_format_json_quote_64bit_floats = 1,
    output_format_json_quote_decimals = 1, date_time_output_format = 'unix_timestamp', output_format_json_named_tuples_as_objects = 0,
    output_format_json_map_as_array_of_tuples = 1, output_format_json_escape_forward_slashes = 0"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE tp_0 (j_x JSON(dt DateTime64(3, 'UTC'), dec Decimal(10, 2), t Tuple(x Int64, y String), m Map(String, Int64))) ENGINE = Memory;
INSERT INTO tp_0 VALUES ('{\"dt\":\"2020-01-02 03:04:05.678\",\"dec\":1.25,\"t\":{\"x\":1,\"y\":\"a/b\"},\"m\":{\"k\":2},\"f\":1.234,\"u\":18446744073709551615,\"i\":-5}');
CREATE TABLE tp (id UInt32, j_x JSON(dt DateTime64(3, 'UTC'), dec Decimal(10, 2), t Tuple(x Int64, y String), m Map(String, Int64)), o_x String) ENGINE = Memory;
INSERT INTO tp SELECT 1, j_x, j_x::String FROM tp_0;
"
{
    for f in JSONHas JSONLength JSONKey JSONType JSONExtractInt JSONExtractUInt JSONExtractFloat JSONExtractBool JSONExtractString JSONExtractRaw \
        JSONExtractArrayRaw JSONExtractKeysAndValuesRaw JSONExtractKeys; do
        for p in "" "'dt'" "'dec'" "'t'" "'m'" "'f'" "'u'" "'i'" "'t', 'y'"; do printf '%s|%s|\n' "$f" "${p:+, $p}"; done
    done
    for p in "" "'dt'" "'dec'" "'t'" "'m'" "'f'" "'u'"; do printf 'JSONExtract|%s|String\nJSONExtract|%s|Dynamic\n' "${p:+, $p}" "${p:+, $p}"; done
} | build_queries tp "SETTINGS $PRESENTATION" j_x | $CLICKHOUSE_CLIENT

# Whole-number, negative-zero, large and narrow floats keep their type and value; non-finite values are strings.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE tf_0 (id UInt32, s String) ENGINE = Memory;
INSERT INTO tf_0 VALUES (1, '{\"f\":1.0,\"f32\":0.0,\"fx\":0.0}'), (2, '{\"f\":-0.0,\"f32\":1.5,\"fx\":2.0}'),
    (3, '{\"f\":1e20,\"f32\":-0.0,\"fx\":-0.0}'), (4, '{\"f\":0.1,\"f32\":1234567808.0,\"fx\":1e300}'),
    (5, '{\"f\":[1.0,-2.5],\"f32\":0.5,\"fx\":\"inf\"}'), (6, '{\"f32\":16777216.0,\"fx\":\"-inf\"}'), (7, '{\"f32\":0.25,\"fx\":\"nan\"}');
CREATE TABLE tf (id UInt32, j_x JSON(f32 Float32, fx Float64), o_x String) ENGINE = Memory;
INSERT INTO tf SELECT id, s, s FROM tf_0;
"
{
    for f in JSONType JSONExtractBool JSONExtractFloat JSONExtractInt JSONExtractRaw JSONExtractString; do
        for p in "'f'" "'f', 1" "'f32'" "'fx'"; do printf '%s|%s|\n' "$f" ", $p"; done
    done
    for p in "'f'" "'f32'" "'fx'"; do printf 'JSONExtract|, %s|Dynamic\n' "$p"; done
} | build_queries tf "" j_x | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT -q "
SELECT '-- comparisons, calls, distinct String results, mismatches';
SELECT tbl, count(), uniqExact(call), uniqExact(e), countIf(r != e) FROM res GROUP BY tbl ORDER BY tbl;
SELECT '-- mismatches';
SELECT tbl, call, col, id, r, e FROM res WHERE r != e ORDER BY ALL FORMAT TSVRaw;
"

# A constant JSON document with non-constant keys is serialized once, not once per row.
$CLICKHOUSE_CLIENT -q "
SELECT '-- constant JSON argument';
WITH concat('{\"a\":7,\"d\":[1,2,3],\"pad\":\"', repeat('x', 1000000), '\"}') AS s, s::JSON AS j
SELECT count(), countIf(JSONExtractInt(j, materialize(['a', 'd', 'zz'][number % 3 + 1])) != JSONExtractInt(s, materialize(['a', 'd', 'zz'][number % 3 + 1]))),
    countIf(JSONExtractInt(j, 'd', materialize(number % 5)) != JSONExtractInt(s, 'd', materialize(number % 5))),
    sum(JSONExtractInt(j, 'd', materialize(number % 5)))
FROM numbers(256) SETTINGS max_execution_time = 10;
"

# Prints each call on the JSON document, then on the same document as String (and with a third column: as `j::String`).
examples()
{
    local doc=$1 call list=""
    shift
    for call in "$@"; do
        sql_quote "$call"
        list+=", ($REPLY, toString(${call}), toString(${call/(j/(s}))"
    done
    echo "SELECT x.1, x.2, x.3 FROM (WITH '$doc' AS s, s::JSON AS j SELECT [${list#, }] AS xs) ARRAY JOIN xs AS x FORMAT TSVRaw;"
}

{
    echo "SELECT '-- examples: the call, on JSON, on String';"
    examples '{"a":{"b":"x","c":1},"d":[1,2],"s":"hi"}' "JSONLength(j)" "JSONLength(j, 'a')" "JSONType(j, 'a')" "JSONType(j, 'd')" \
        "JSONExtractKeys(j)" "JSONExtractKeysAndValuesRaw(j)" "JSONExtractStringCaseInsensitive(j, 'S')" "JSONExtractRawCaseInsensitive(j, 'A')" \
        "isValidJSON(j)" "JSONExtractKeys(j, 'a')" "JSONExtractKeysCaseInsensitive(j, 'A')" "JSONExtractArrayRaw(j, 'd')" \
        "JSONExtract(j, 'd', 'Array(Int64)')" "JSONExtractKeysAndValues(j, 'a', 'String')" "JSONKey(j, 1)" "JSONHas(j, 'd', 1)" \
        "JSONExtractInt(j, 'd', 1)"
    echo "SELECT '-- https://github.com/ClickHouse/ClickHouse/issues/102079';"
    examples '{"x": {"a": 5, "b": 7, "c": 11}}' "JSONExtractKeysAndValues(j, 'x', 'Int8')"
    examples '{"a": "hello", "b": [-100, 200.0, 300]}' "JSONExtract(j, 'Tuple(String, Array(Float64))')" "JSONExtractKeys(j)"
    echo "SELECT '-- value extraction';"
    examples '{"f":1.5,"t":true,"str":"hi","o":{"n":5,"m":6}}' "JSONExtractInt(j, 'f')" "JSONExtractFloat(j, 't')" \
        "JSONExtract(j, 'str', 'LowCardinality(String)')" "JSONExtract(j, 'missing', 'Enum8(\\'x\\' = 1, \\'hi\\' = 2)')" \
        "JSONExtract(j, 'o', 'Tuple(n UInt8, m UInt8)')"
    echo "SELECT '-- what the JSON type itself normalizes: the call, on JSON, on j::String, on the source document';"
    echo "WITH '{\"z\":null,\"o\":{},\"b\":1,\"a\":{\"c\":2},\"a.b\":3}' AS s, s::JSON AS j SELECT j::String;"
    for call in "JSONHas(j, 'z')" "JSONHas(j, 'o')" "JSONExtractKeys(j)" "JSONExtractInt(j, 'a.b')" "JSONExtractInt(j, 'a', 'b')"; do
        sql_quote "$call"
        echo "WITH '{\"z\":null,\"o\":{},\"b\":1,\"a\":{\"c\":2},\"a.b\":3}' AS s, s::JSON AS j
            SELECT $REPLY, toString($call), toString(${call/(j/(j::String}), toString(${call/(j/(s}) FORMAT TSVRaw;"
    done
} | $CLICKHOUSE_CLIENT
