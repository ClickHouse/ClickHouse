#!/usr/bin/env bash
# `EXPLAIN SYNTAX` and `EXPLAIN AST optimize = 1` show a parameterized view's definition only to a user
# with `SHOW COLUMNS` on the view, which is what `SHOW CREATE` requires.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${user};
    CREATE TABLE base (x UInt32, s String) ENGINE = MergeTree ORDER BY x;
    CREATE VIEW pv SQL SECURITY INVOKER AS SELECT x, s FROM base WHERE x < {n:UInt32} AND s != 'definition_marker';
    CREATE USER ${user} NOT IDENTIFIED;
"

# Number of output lines that carry the literal from the view definition and that carry the view call.
# A failed query prints its error code instead.
explain_as_user()
{
    local out
    if ! out=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "$1" 2>&1); then
        echo "error: $(grep -oE 'Code: [0-9]+' <<< "$out" | head -n 1)"
        return
    fi
    echo "definition $(grep -c 'definition_marker' <<< "$out"), call $(grep -cE 'pv.?\(|Function [^ ]+\.pv \(' <<< "$out")"
}

echo "-- no grants"
${CLICKHOUSE_CLIENT} --user "${user}" --query "EXPLAIN SYNTAX oneline = 1 SELECT * FROM pv(n = 3)"
explain_as_user "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 3)"

echo "-- SELECT on the table behind the view, no grant on the view"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base TO ${user}"
explain_as_user "EXPLAIN SYNTAX SELECT * FROM pv(n = 3)"
explain_as_user "EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM pv(n = 3)"
explain_as_user "EXPLAIN AST optimize = 1 SELECT * FROM pv(n = 3)"

echo "-- SHOW COLUMNS on the view"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.pv TO ${user}"
explain_as_user "EXPLAIN SYNTAX SELECT * FROM pv(n = 3)"

echo "-- SELECT(x) on the view"
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.pv FROM ${user}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(x) ON ${CLICKHOUSE_DATABASE}.pv TO ${user}"
explain_as_user "EXPLAIN SYNTAX SELECT * FROM pv(n = 3)"

echo "-- SELECT on the view"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv TO ${user}"
explain_as_user "EXPLAIN SYNTAX SELECT * FROM pv(n = 3)"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
