#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: the replica parses SQL DDL with `dialect = clickhouse_json`, https://github.com/ClickHouse/ClickHouse/issues/123323

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

JSON_URL="${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP DICTIONARY IF EXISTS dictionary_separator_rejected;
        DROP DICTIONARY IF EXISTS dictionary_separator_valid;
        DROP TABLE IF EXISTS dictionary_separator_src;"
}
trap cleanup EXIT
cleanup

to_json()
{
    ${CLICKHOUSE_CLIENT} --param_statement "$1" \
        --query "SELECT parseQueryToJSON({statement:String}) FORMAT TSVRaw"
}

change_separator()
{
    local json before after
    json=$(to_json "$1")
    before="$2,\"separator\":\"\\u0000\""
    after="$2"
    if [[ -n "$3" ]]; then
        after+=",\"separator\":$3"
    fi
    if [[ "$json" != *"$before"* ]]; then
        printf 'Missing pair-list separator in generated JSON\n' >&2
        return 1
    fi
    printf '%s\n' "${json/"$before"/"$after"}"
}

expect_bad_arguments()
{
    local response
    response=$(${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary "$1")
    if ! grep -qF '(BAD_ARGUMENTS)' <<< "$response" || ! grep -qF "$3" <<< "$response"; then
        printf 'Expected BAD_ARGUMENTS for %s, got: %s\n' "$2" "$response" >&2
        return 1
    fi
    printf '%s\n' "$2"
    ${CLICKHOUSE_CLIENT} --query "EXISTS DICTIONARY dictionary_separator_rejected"
}

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE dictionary_separator_src (prefix String, value UInt64) ENGINE = Memory;
    INSERT INTO dictionary_separator_src VALUES ('1.2.3.0/24', 123);"

# Preserve the NUL separator of pair lists without changing ordinary comma-separated expression lists.
valid_sql="CREATE DICTIONARY dictionary_separator_valid (prefix String, value UInt64) PRIMARY KEY prefix SOURCE(CLICKHOUSE(DB '${CLICKHOUSE_DATABASE}' TABLE 'dictionary_separator_src')) LAYOUT(IP_TRIE(ACCESS_TO_KEY_FROM_ATTRIBUTES 1 RADIX_PREFIX)) LIFETIME(0)"
${CLICKHOUSE_CLIENT} --enable_json_ast_dialect 1 --param_statement "$valid_sql" --query "
    SELECT formatQueryFromJSON(parseQueryToJSON({statement:String})) = formatQuerySingleLine({statement:String})"
${CLICKHOUSE_CURL} -fsS "$JSON_URL" --data-binary "$(to_json "$valid_sql")"
${CLICKHOUSE_CLIENT} --query "SELECT dictGetUInt64('dictionary_separator_valid', 'value', toIPv4('1.2.3.4'))"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE DICTIONARY dictionary_separator_valid" |
    grep -oF 'LAYOUT(IP_TRIE(ACCESS_TO_KEY_FROM_ATTRIBUTES 1 RADIX_PREFIX))'
${CLICKHOUSE_CLIENT} --multiquery --query "
    DETACH DICTIONARY dictionary_separator_valid;
    ATTACH DICTIONARY dictionary_separator_valid;
    SELECT dictGetUInt64('dictionary_separator_valid', 'value', toIPv4('1.2.3.4'));"

# Formatting a supported nested source needs no HTTP service. Its two-column key retains comma separation.
nested_sql="CREATE DICTIONARY dictionary_separator_rejected (id UInt64, key String, value String) PRIMARY KEY id, key SOURCE(HTTP(URL 'http://127.0.0.1/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'X-One' VALUE 'one') HEADER(NAME 'X-Two' VALUE 'two')))) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0)"
empty_sql="CREATE DICTIONARY d (prefix String, value UInt64) PRIMARY KEY prefix SOURCE(NULL()) LAYOUT(IP_TRIE(RADIX_PREFIX)) LIFETIME(0)"
for statement in "$nested_sql" "$empty_sql"; do
    ${CLICKHOUSE_CLIENT} --enable_json_ast_dialect 1 --param_statement "$statement" --query "
        SELECT formatQueryFromJSON(parseQueryToJSON({statement:String})) = formatQuerySingleLine({statement:String})"
done

# Send JSON directly to the server so rejection cannot happen only in the client.
# Each changed list contains at least two pairs: accepting it would store SQL which cannot be parsed.
rejected_sql="${valid_sql/dictionary_separator_valid/dictionary_separator_rejected}"
layout_list='"parameters":{"type":"ExpressionList"'
source_list='"elements":{"type":"ExpressionList"'
nested_list='"first":"header","second_with_brackets":true,"second":{"type":"ExpressionList"'
expect_bad_arguments "$(change_separator "$rejected_sql" "$layout_list" '","')" 'Comma in layout parameters' "'parameters' of ASTDictionaryLayout must use the NUL separator"
expect_bad_arguments "$(change_separator "$rejected_sql" "$source_list" '","')" 'Comma in source parameters' "'elements' of ASTFunctionWithKeyValueArguments must use the NUL separator"
expect_bad_arguments "$(change_separator "$nested_sql" "$nested_list" '","')" 'Comma in nested source parameters' "'second' of ASTPair must use the NUL separator"

# An absent separator defaults to comma, and a different nonzero separator is equally invalid.
expect_bad_arguments "$(change_separator "$rejected_sql" "$source_list" '')" 'Missing source separator' "'elements' of ASTFunctionWithKeyValueArguments must use the NUL separator"
expect_bad_arguments "$(change_separator "$rejected_sql" "$layout_list" '";"')" 'Semicolon in layout parameters' "'parameters' of ASTDictionaryLayout must use the NUL separator"
