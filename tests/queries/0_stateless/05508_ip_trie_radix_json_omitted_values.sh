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
        DROP DICTIONARY IF EXISTS ip_trie_json_rejected;
        DROP DICTIONARY IF EXISTS ip_trie_json_valid;
        DROP TABLE IF EXISTS ip_trie_json_src;"
}
trap cleanup EXIT
cleanup

to_json()
{
    ${CLICKHOUSE_CLIENT} --param_statement "$1" \
        --query "SELECT parseQueryToJSON({statement:String}) FORMAT TSVRaw"
}

omit_value()
{
    local json key
    json=$(to_json "$1")
    key="\"first\":\"$2\""
    [[ "$json" == *"$key"* ]]
    printf '%s\n' "${json/"$key"/"$key,\"value_omitted\":true"}"
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
    ${CLICKHOUSE_CLIENT} --query "EXISTS DICTIONARY ip_trie_json_rejected"
}

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE ip_trie_json_src (prefix String, value UInt64) ENGINE = Memory;
    INSERT INTO ip_trie_json_src VALUES ('1.2.3.0/24', 123);"

# JSON reaches the server directly: `clickhouse-client` must not reject it before deserialization there.
# The supported omission remains readable after storing and reattaching the dictionary.
valid_sql="CREATE DICTIONARY ip_trie_json_valid (prefix String, value UInt64) PRIMARY KEY prefix SOURCE(CLICKHOUSE(DB '${CLICKHOUSE_DATABASE}' TABLE 'ip_trie_json_src')) LAYOUT(IP_TRIE(ACCESS_TO_KEY_FROM_ATTRIBUTES 1 RADIX_PREFIX)) LIFETIME(0)"
${CLICKHOUSE_CLIENT} --enable_json_ast_dialect 1 --param_statement "$valid_sql" --query "
    SELECT formatQueryFromJSON(parseQueryToJSON({statement:String})) = formatQuerySingleLine({statement:String})"
${CLICKHOUSE_CURL} -fsS "$JSON_URL" --data-binary "$(to_json "$valid_sql")"
${CLICKHOUSE_CLIENT} --query "SELECT dictGetUInt64('ip_trie_json_valid', 'value', toIPv4('1.2.3.4'))"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE DICTIONARY ip_trie_json_valid" |
    grep -oF 'LAYOUT(IP_TRIE(ACCESS_TO_KEY_FROM_ATTRIBUTES 1 RADIX_PREFIX))'
${CLICKHOUSE_CLIENT} --multiquery --query "
    DETACH DICTIONARY ip_trie_json_valid;
    ATTACH DICTIONARY ip_trie_json_valid;
    SELECT dictGetUInt64('ip_trie_json_valid', 'value', toIPv4('1.2.3.4'));"

# A hidden source value used to be consumed by the configuration builder but omitted from stored SQL.
prefix="CREATE DICTIONARY ip_trie_json_rejected (prefix String, value UInt64) PRIMARY KEY prefix"
source="SOURCE(CLICKHOUSE(DB '${CLICKHOUSE_DATABASE}' TABLE 'ip_trie_json_src'))"
suffix="LAYOUT(IP_TRIE()) LIFETIME(0)"
expect_bad_arguments "$(omit_value "$prefix $source $suffix" table)" 'Hidden source value' 'An omitted ASTPair value must be a NULL literal'

# A dummy `NULL` satisfies the pair invariant, but omission is still forbidden in every source pair.
expect_bad_arguments "$(omit_value "$prefix SOURCE(CLICKHOUSE(TABLE NULL)) $suffix" table)" 'Omitted source value' "Dictionary source parameter 'table' must have a value"
expect_bad_arguments "$(omit_value "$prefix SOURCE(CLICKHOUSE(TABLE 'ip_trie_json_src' REPLICA(HOST NULL))) $suffix" host)" 'Omitted nested source value' "Dictionary source parameter 'host' must have a value"

# Omitted layout values must also carry the dummy `NULL`, never a hidden literal or identifier.
expect_bad_arguments "$(omit_value "$prefix $source LAYOUT(IP_TRIE(RADIX_PREFIX 8)) LIFETIME(0)" radix_prefix)" 'Hidden layout value' 'An omitted ASTPair value must be a NULL literal'
expect_bad_arguments "$(omit_value "$prefix $source LAYOUT(IP_TRIE(RADIX_PREFIX ignored)) LIFETIME(0)" radix_prefix)" 'Hidden layout identifier' 'An omitted ASTPair value must be a NULL literal'

# Only the supported layout parameter may omit a value.
expect_bad_arguments "$(to_json "$prefix $source LAYOUT(IP_TRIE(OTHER_PARAMETER)) LIFETIME(0)")" 'Other omitted layout parameter' "Dictionary layout parameter 'other_parameter' of layout 'ip_trie' must have a value"
expect_bad_arguments "$(to_json "CREATE DICTIONARY ip_trie_json_rejected (id UInt64, value UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT(RADIX_PREFIX)) LIFETIME(0)")" 'Omission in another layout' "Dictionary layout parameter 'radix_prefix' of layout 'flat' must have a value"

# SQL admits the omitted parameter only immediately before `)`. JSON must not store an unparsable order.
expect_bad_arguments "$(omit_value "$prefix $source LAYOUT(IP_TRIE(RADIX_PREFIX NULL ACCESS_TO_KEY_FROM_ATTRIBUTES 1)) LIFETIME(0)" radix_prefix)" 'Omitted parameter before another parameter' "Dictionary layout parameter 'radix_prefix' without a value must be the last parameter"
