#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: uses SQL user-defined functions and HTTP handlers with users

# The query of a SQL-defined HTTP handler is checked against the privileges of the user who creates or alters it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
DB_B="${DB}_b"
BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
U="u_lim_${DB}"
U_ALL="u_all_${DB}"
F="f_${DB}"

cleanup() {
    for h in $($CLICKHOUSE_CLIENT -q "SELECT name FROM system.handlers WHERE endsWith(name, '_${DB}')"); do
        $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$h\`"
    done
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${U}, ${U_ALL}; DROP FUNCTION IF EXISTS ${F}; DROP DATABASE IF EXISTS ${DB_B}"
}
trap cleanup EXIT
cleanup

# Prints OK, or the name of the error.
run() {
    local out
    if out=$("$@" 2>&1); then
        echo OK
    else
        echo "$out" | grep -o -m1 -E '\((ACCESS_DENIED|HANDLER_ALREADY_EXISTS|UNKNOWN_TABLE)\)' || echo "$out"
    fi
}
as_user() { run $CLICKHOUSE_CLIENT --user "$1" -q "$2"; }
as_default() { run $CLICKHOUSE_CLIENT -q "$1"; }
handlers() { $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.handlers WHERE name = '$1'"; }
handler_query() { $CLICKHOUSE_CLIENT -q "SELECT replaceAll($2, '${DB}', 'DB') FROM system.handlers WHERE name = '$1'"; }

$CLICKHOUSE_CLIENT -q "
CREATE TABLE ${DB}.t (id UInt32, v String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB}.t VALUES (1, 'a');
CREATE TABLE ${DB}.t2 (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB}.t2 VALUES (5);
CREATE USER ${U} NOT IDENTIFIED;
GRANT CREATE HANDLER, ALTER HANDLER, DROP HANDLER ON *.* TO ${U};"

echo "--- 1. a query over a table the creator cannot read"
as_user "$U" "CREATE HANDLER h1_${DB} URL '/${DB}/h1' AS SELECT * FROM ${DB}.t"
handlers "h1_${DB}"

echo "--- 2. with the grant; the handler keeps serving after a revoke"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${DB}.t TO ${U}"
as_user "$U" "CREATE HANDLER h1_${DB} URL '/${DB}/h1' AS SELECT * FROM ${DB}.t"
${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/h1"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON ${DB}.t FROM ${U}"
${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/h1"

echo "--- 3. column grants"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(id) ON ${DB}.t TO ${U}"
as_user "$U" "CREATE HANDLER h3a_${DB} URL '/${DB}/h3a' AS SELECT v FROM ${DB}.t"
as_user "$U" "CREATE HANDLER h3b_${DB} URL '/${DB}/h3b' AS SELECT id FROM ${DB}.t"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${DB}.t TO ${U}"

echo "--- 4. subqueries and joins"
as_user "$U" "CREATE HANDLER h4a_${DB} URL '/${DB}/h4a' AS SELECT 1 WHERE 1 IN (SELECT id FROM ${DB}.t2)"
as_user "$U" "CREATE HANDLER h4b_${DB} URL '/${DB}/h4b' AS SELECT * FROM ${DB}.t JOIN ${DB}.t2 USING (id)"

echo "--- 5. INSERT needs DEFINER = CURRENT_USER, and then runs with the privileges of the creator"
$CLICKHOUSE_CLIENT -q "GRANT INSERT ON ${DB}.t TO ${U}"
as_user "$U" "CREATE HANDLER h5a_${DB} URL '/${DB}/h5a' METHODS (POST) AS INSERT INTO ${DB}.t FORMAT TSV"
as_user "$U" "CREATE HANDLER h5b_${DB} URL '/${DB}/h5b' METHODS (POST) AS INSERT INTO ${DB}.t SELECT * FROM ${DB}.t"
as_user "$U" "CREATE HANDLER h5a_${DB} URL '/${DB}/h5a' METHODS (POST) DEFINER = CURRENT_USER SQL SECURITY DEFINER AS INSERT INTO ${DB}.t FORMAT TSV"
as_user "$U" "CREATE HANDLER h5b_${DB} URL '/${DB}/h5b' METHODS (POST) DEFINER = CURRENT_USER SQL SECURITY DEFINER AS INSERT INTO ${DB}.t SELECT id + 10, v FROM ${DB}.t WHERE id = 1"
printf '2\tb\n' | ${CLICKHOUSE_CURL} -sS -X POST --data-binary @- "${BASE}/${DB}/h5a"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}/${DB}/h5b"
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${DB}.t ORDER BY id"
$CLICKHOUSE_CLIENT -q "REVOKE INSERT ON ${DB}.t FROM ${U}"
printf '3\tc\n' | ${CLICKHOUSE_CURL} -sS -X POST --data-binary @- "${BASE}/${DB}/h5a" | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${DB}.t"

echo "--- 6. statements that are not SELECT"
$CLICKHOUSE_CLIENT --user "$U" -q "CREATE HANDLER h6_${DB} URL '/${DB}/h6' METHODS (POST) AS GRANT SELECT ON ${DB}.t TO ${U}" 2>&1 \
    | grep -c -m1 "runs a GRANT query"
$CLICKHOUSE_CLIENT --user "$U" -q "CREATE HANDLER h6b_${DB} URL '/${DB}/h6b' METHODS (POST) AS SYSTEM FLUSH LOGS" 2>&1 \
    | grep -c -m1 "runs a SYSTEM query"
handlers "h6b_${DB}"

echo "--- 7. PARALLEL WITH"
as_user "$U" "CREATE HANDLER h7_${DB} URL '/${DB}/h7' AS SELECT 1 PARALLEL WITH SELECT * FROM ${DB}.t2"

echo "--- 8. query parameters"
as_user "$U" "CREATE HANDLER h8a_${DB} URL '/${DB}/h8a' AS SELECT * FROM ${DB}.t WHERE id = {id:UInt32}"
as_user "$U" "CREATE HANDLER h8b_${DB} URL '/${DB}/h8b' AS SELECT * FROM {tbl:Identifier}"
as_user "$U" "CREATE HANDLER h8c_${DB} URL '/${DB}/h8c' AS SELECT * FROM merge('${DB}', if(empty({r:String}), '^t\$', {r:String}))"
as_user "$U" "CREATE HANDLER h8d_${DB} URL REGEXP '/${DB}/h8d/(?P<id>\\\\d+)' AS SELECT * FROM ${DB}.t WHERE id = {id:UInt32}"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.handlers WHERE name LIKE 'h8%' AND endsWith(name, '_${DB}')"
as_default "CREATE HANDLER h8a_${DB} URL '/${DB}/h8a' AS SELECT * FROM ${DB}.t WHERE id = {id:UInt32}"
as_default "CREATE HANDLER h8b_${DB} URL '/${DB}/h8b' AS SELECT * FROM {tbl:Identifier}"
as_default "CREATE HANDLER h8c_${DB} URL '/${DB}/h8c' AS SELECT * FROM merge('${DB}', if(empty({r:String}), '^t\$', {r:String}))"
as_user "$U" "CREATE HANDLER h8e_${DB} URL '/${DB}/h8e' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM ${DB}.t WHERE id = {id:UInt32}"
${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/h8e?param_id=1"

echo "--- 9. ALTER of the query"
as_user "$U" "CREATE HANDLER h9_${DB} URL '/${DB}/h9' AS SELECT 1"
as_user "$U" "ALTER HANDLER h9_${DB} AS SELECT * FROM ${DB}.t2"
handler_query "h9_${DB}" query

echo "--- 10. ALTER of another clause"
as_default "CREATE HANDLER h10_${DB} URL '/${DB}/h10' AS SELECT * FROM ${DB}.t2"
as_user "$U" "ALTER HANDLER h10_${DB} URL '/${DB}/h10b'"
handler_query "h10_${DB}" url

echo "--- 11. a user with full access is not restricted, and the query is stored as is"
as_default "CREATE HANDLER h11a_${DB} URL '/${DB}/h11a' METHODS (POST) AS GRANT SELECT ON ${DB}.t TO ${U}"
as_default "CREATE HANDLER h11b_${DB} URL '/${DB}/h11b' METHODS (POST) AS SYSTEM FLUSH LOGS"
as_default "CREATE HANDLER h11c_${DB} URL '/${DB}/h11c' AS SELECT * FROM t2"
handler_query "h11c_${DB}" create_query
$CLICKHOUSE_CLIENT -q "CREATE USER ${U_ALL} NOT IDENTIFIED; GRANT ALL ON *.* TO ${U_ALL}"
echo "ALL without GRANT OPTION:"
as_user "$U_ALL" "CREATE HANDLER h11d_${DB} URL '/${DB}/h11d' METHODS (POST) AS SYSTEM FLUSH LOGS"
as_user "$U_ALL" "CREATE HANDLER h11e_${DB} URL '/${DB}/h11e' AS SELECT * FROM ${DB}.t2"

echo "--- 12. the names are qualified with the database of the creator"
$CLICKHOUSE_CLIENT -q "
CREATE DATABASE ${DB_B};
CREATE TABLE ${DB_B}.t (id UInt32, v String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB_B}.t VALUES (9, 'other');"
as_user "$U" "CREATE HANDLER h12_${DB} URL '/${DB}/h12' AS SELECT * FROM t WHERE id = 1"
handler_query "h12_${DB}" query
${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/h12?database=${DB_B}"
${CLICKHOUSE_CURL} -sS -H "X-ClickHouse-Database: ${DB_B}" "${BASE}/${DB}/h12"
as_user "$U" "CREATE HANDLER h12b_${DB} URL '/${DB}/h12b' AS WITH x AS (SELECT id FROM t WHERE id = 1) SELECT * FROM x"
handler_query "h12b_${DB}" query
${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/h12b?database=${DB_B}"

echo "--- 13. SETTINGS clause"
as_user "$U" "CREATE HANDLER h13a_${DB} URL '/${DB}/h13a' AS SELECT * FROM ${DB}.t SETTINGS max_threads = 1"
as_user "$U" "CREATE HANDLER h13b_${DB} URL '/${DB}/h13b' AS SELECT 1 SETTINGS additional_result_filter = '1 IN (SELECT id FROM ${DB}.t2)'"
as_default "CREATE HANDLER h13a_${DB} URL '/${DB}/h13a' AS SELECT * FROM ${DB}.t SETTINGS max_threads = 1"
as_default "CREATE HANDLER h13b_${DB} URL '/${DB}/h13b' AS SELECT 1 SETTINGS additional_result_filter = '1 IN (SELECT id FROM ${DB}.t2)'"

echo "--- 14. CREATE HANDLER IF NOT EXISTS of an existing handler"
as_default "CREATE HANDLER h14_${DB} URL '/${DB}/h14' AS SELECT 1"
as_user "$U" "CREATE HANDLER IF NOT EXISTS h14_${DB} URL '/${DB}/h14' AS SELECT * FROM ${DB}.t2"
handler_query "h14_${DB}" query
as_user "$U" "CREATE HANDLER h14_${DB} URL '/${DB}/h14' AS SELECT * FROM ${DB}.t2"

echo "--- 16. SQL user-defined functions are expanded"
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION ${F} AS () -> 1"
as_user "$U" "CREATE HANDLER h16_${DB} URL '/${DB}/h16' AS SELECT ${F}() AS x"
as_default "CREATE HANDLER h16b_${DB} URL '/${DB}/h16b' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT ${F}() AS x"
$CLICKHOUSE_CLIENT -q "SELECT name, countSubstrings(create_query, '${F}') FROM system.handlers WHERE name IN ('h16_${DB}', 'h16b_${DB}') ORDER BY name" \
    | sed "s/_${DB}//"
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE FUNCTION ${F} AS () -> (SELECT count() + 100 FROM ${DB}.t2)"
${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/h16"
${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/h16b"
