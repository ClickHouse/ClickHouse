#!/usr/bin/env bash

# The query plan cache must not reuse a plan whose result depends on state that is not a part
# of the cache key: the requested processing stage, nondeterministic functions that are only
# visible after analysis, and formatting settings captured by functions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

suffix="${CLICKHOUSE_DATABASE//[^a-zA-Z0-9_]/_}"
user_a="qpc_hidden_a_${suffix}"
user_b="qpc_hidden_b_${suffix}"
table="qpc_hidden_${suffix}"
settings="--enable_query_plan_cache=1 --allow_experimental_analyzer=1 --enable_parallel_replicas=0"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_a}"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_b}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_a} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_b} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (a UInt64, who String ALIAS currentUser()) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${table} VALUES (1)"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${table} TO ${user_a}, ${user_b}"

echo "stage: fetch_columns first"
${CLICKHOUSE_CLIENT} ${settings} --stage fetch_columns --query "SELECT a + 1 FROM ${table} WHERE a > 0" >/dev/null
${CLICKHOUSE_CLIENT} ${settings} --query "SELECT a + 1 FROM ${table} WHERE a > 0"
${CLICKHOUSE_CLIENT} ${settings} --query "SELECT a + 1 FROM ${table} WHERE a > 0"

echo "stage: complete first"
${CLICKHOUSE_CLIENT} ${settings} --query "SELECT a + 1 FROM ${table} WHERE a >= 1"
${CLICKHOUSE_CLIENT} ${settings} --stage fetch_columns --query "SELECT a + 1 FROM ${table} WHERE a >= 1"

check_user()
{
    local query="$1"
    for user in "${user_a}" "${user_b}"; do
        result=$(${CLICKHOUSE_CLIENT} --user "${user}" ${settings} --query "${query}")
        if [ "${result}" = "${user}" ]; then
            echo "ok"
        else
            echo "unexpected result '${result}' for '${user}'"
        fi
    done
}

echo "CURRENT_USER identifier"
check_user "SELECT CURRENT_USER FROM ${table}"

echo "ALIAS column with currentUser"
check_user "SELECT who FROM ${table}"

echo "ALIAS column with currentUser in WHERE"
for user in "${user_a}" "${user_b}"; do
    ${CLICKHOUSE_CLIENT} --user "${user}" ${settings} --param_user "${user}" --query "SELECT count() FROM ${table} WHERE who = {user:String}"
done
${CLICKHOUSE_CLIENT} --user "${user_a}" ${settings} --query "SELECT a FROM ${table} WHERE who = '${user_a}'"
${CLICKHOUSE_CLIENT} --user "${user_b}" ${settings} --query "SELECT a FROM ${table} WHERE who = '${user_a}'"

echo "output format settings"
json_query="SELECT toJSONString(toUInt64(1)) AS s FROM ${table}"
${CLICKHOUSE_CLIENT} ${settings} --output_format_json_quote_64bit_integers=1 --query "${json_query}"
${CLICKHOUSE_CLIENT} ${settings} --output_format_json_quote_64bit_integers=0 --query "${json_query}"
${CLICKHOUSE_CLIENT} ${settings} --output_format_json_quote_64bit_integers=1 --query "${json_query}"

echo "trivial count with the default settings"
${CLICKHOUSE_CLIENT} ${settings} --query "SELECT count() FROM ${table}"
${CLICKHOUSE_CLIENT} ${settings} --query "SELECT count() FROM ${table}"
