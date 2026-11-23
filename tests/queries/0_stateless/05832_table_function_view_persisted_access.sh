#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table created with `CREATE TABLE ... AS view(...)` or `viewExplain(...)` creates its storage on the first access, with
# the global context. That must not analyze or explain the query with the global access rights, which would evaluate
# constant expressions, such as `dictGet`, the user has no access to, and disclose their values in an error or a plan.

db_s="${CLICKHOUSE_DATABASE}_s"
user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
secret="SECRET_${CLICKHOUSE_TEST_UNIQUE_NAME}"
dict_get="dictGetString('${db_s}.dict', 'secret', toUInt64(1))"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db_s}"
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} <<EOF
CREATE DATABASE ${db_s};
CREATE TABLE ${db_s}.src (id UInt64, secret String) ENGINE = Memory;
INSERT INTO ${db_s}.src VALUES (1, '${secret}');
CREATE DICTIONARY ${db_s}.dict (id UInt64, secret String DEFAULT '')
    PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src' DB '${db_s}')) LAYOUT(HASHED()) LIFETIME(0);
CREATE USER ${user};
GRANT CREATE TABLE, DROP TABLE, SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user};
EOF

output=""

# Prints the error code names only. The whole output is kept, to check at the end that the secret is not in it.
run_as_user()
{
    local result
    result=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "$1" 2>&1)
    output+="${result}"
    echo "${result}" | grep -o -m1 -E "\(([A-Z_]+)\)" | tr -d '()'
}

echo "-- dictGet directly"
run_as_user "SELECT ${dict_get}"

echo "-- view with columns"
run_as_user "CREATE TABLE v (leak UInt64) AS view(SELECT toUInt64(concat('x', ${dict_get})) AS leak)"
run_as_user "SELECT * FROM v"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE v"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE v"
run_as_user "SELECT * FROM v"

echo "-- view with columns converted from the query"
run_as_user "CREATE TABLE v_ok (x String) AS view(SELECT number AS x FROM numbers(3))"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT x, toTypeName(x) FROM v_ok ORDER BY x"

echo "-- view with a Nullable result of a join converted to the declared column, also after the table is loaded again"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE v_join (x UInt8) AS view(
        SELECT r.dummy AS x FROM system.one AS l LEFT JOIN system.one AS r ON l.dummy = r.dummy SETTINGS join_use_nulls = 1)"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM v_join"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE v_join"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE v_join"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM v_join"

echo "-- viewExplain"
run_as_user "CREATE TABLE e (explain String) AS viewExplain('EXPLAIN', 'actions = 1', (SELECT ${dict_get} AS leak))"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS e"
run_as_user "CREATE TABLE e AS viewExplain('EXPLAIN', 'actions = 1', (SELECT ${dict_get} AS leak))"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS e"
# Nested in a table function that can be persisted.
run_as_user "CREATE TABLE e (explain String) AS remote('127.0.0.1', viewExplain('EXPLAIN', 'actions = 1', (SELECT ${dict_get} AS leak)))"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count() > 0 FROM (EXPLAIN SELECT 1)"

echo "-- the secret in the output:"
echo "${output}" | grep -c "${secret}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS v"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS v_ok"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS v_join"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS e"
