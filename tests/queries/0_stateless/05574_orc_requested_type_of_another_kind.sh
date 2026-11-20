#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

# An ORC list, map or struct requested as a type of another kind is cast to it, not read as default values.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Prints the error code and the conversion of a query that has to fail.
function error()
{
    $CLICKHOUSE_LOCAL -q "$1" 2>&1 | tr '\n' ' ' | grep -o -E 'Code: [0-9]+|while converting column [^:]* to type [^:]*' | head -n 2 | paste -sd ' '
}

FILE="$(realpath "$CLICKHOUSE_TMP")/${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"
$CLICKHOUSE_LOCAL -q "SELECT 2::Int64 AS id, map('key', 'profile') AS m, ['profile'] AS l,
    CAST(tuple('profile'), 'Tuple(value String)') AS s, map('k', CAST(tuple('v'), 'Tuple(value String)')) AS ms,
    [map('k', 'v')] AS lm, CAST(tuple(['x']), 'Tuple(a Array(String))') AS sl FORMAT ORC" > "$FILE"
F="file('$FILE', ORC"

# Conversions the cast cannot do.
error "SELECT * FROM $F, 'id Int64, m Tuple(value Nullable(String))')"
error "SELECT * FROM $F, 'id Int64, m Tuple(value Nullable(String))') SETTINGS input_format_orc_allow_missing_columns = 0"
error "SELECT * FROM $F, 'id Int64, l Tuple(value Nullable(String))')"
error "SELECT * FROM $F, 'id Int64, s Map(String, String)')"
error "SELECT * FROM $F, 'id Int64, s Array(String)')"
error "SELECT * FROM $F, 'id Int64, lm Array(Tuple(value String))')"
error "SELECT * FROM $F, 'id Int64, sl Tuple(a Map(String, String))')"

# Conversions the cast does, also of a map value.
$CLICKHOUSE_LOCAL -q "SELECT * FROM $F, 'id Int64, m String')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM $F, 'id Int64, m Array(Tuple(String, String))')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM $F, 'id Int64, ms Map(String, String)')"
$CLICKHOUSE_LOCAL -q "SELECT ms.values FROM $F, 'id Int64, ms Map(String, String)')"

# Matching kinds, a Tuple naming only some of the fields, and the other columns of a mismatched structure.
$CLICKHOUSE_LOCAL -q "SELECT * FROM $F, 'id Int64, m Map(String, String), l Array(String), s Tuple(value String),
    ms Map(String, Tuple(value String)), lm Array(Map(String, String)), sl Tuple(a Array(String))')"
$CLICKHOUSE_LOCAL -q "SELECT s FROM $F, 's Tuple(value String, other Int32)')"
$CLICKHOUSE_LOCAL -q "SELECT id FROM $F, 'id Int64, m Tuple(value Nullable(String))')"

DICT_FILE="$(realpath "$CLICKHOUSE_TMP")/${CLICKHOUSE_TEST_UNIQUE_NAME}_dict.orc"
$CLICKHOUSE_LOCAL --output_format_orc_dictionary_key_size_threshold=1 -q "SELECT map('key', 'profile') AS m, [map('k', 'v')] AS lm FORMAT ORC" > "$DICT_FILE"
D="file('$DICT_FILE', ORC"
S="SETTINGS input_format_orc_dictionary_as_low_cardinality = 1"

# A dictionary-encoded map key.
error "SELECT * FROM $D, 'm Tuple(value Nullable(String))') $S"
error "SELECT * FROM $D, 'lm Array(Tuple(value String))') $S"
$CLICKHOUSE_LOCAL -q "SELECT * FROM $D, 'm String') $S"
$CLICKHOUSE_LOCAL -q "SELECT * FROM $D, 'm Map(String, String), lm Array(Map(String, String))') $S"

rm -f "$FILE"
rm -f "$DICT_FILE"
