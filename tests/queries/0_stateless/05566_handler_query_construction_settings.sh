#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The query-construction settings (`limit`, `page`, `select`, `filter`, `order`) in the `SETTINGS` clause of the
# query of `CREATE HANDLER` / `ALTER HANDLER` shape the handler's result, as for the same query sent directly.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URLs: handlers and users are global.
DB="${CLICKHOUSE_DATABASE}"
APP_USER="u_05566_${DB}"
PROFILE="p_05566_${DB}"

cleanup() {
    local drops=""
    for i in 1 3 4 5 6 7 8 9 10 11 12 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29; do
        drops+="DROP HANDLER IF EXISTS h${i}_${DB}; "
    done
    ${CLICKHOUSE_CLIENT} -q "${drops}DROP USER IF EXISTS ${APP_USER}; DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE ${DB}.t (a UInt8) ENGINE = Memory;
INSERT INTO ${DB}.t SELECT number + 1 FROM numbers(3);
CREATE USER ${APP_USER};
GRANT SELECT ON ${DB}.t TO ${APP_USER};
CREATE HANDLER h1_${DB} URL '/h1_${DB}' AS SELECT a FROM ${DB}.t ORDER BY a SETTINGS limit = 1;
CREATE HANDLER h3_${DB} URL '/h3_${DB}' AS SELECT a FROM ${DB}.t ORDER BY a SETTINGS limit = 1, page = 3;
CREATE HANDLER h4_${DB} URL '/h4_${DB}' AS SELECT a FROM ${DB}.t SETTINGS select = 'a * 10', filter = 'a > 1', order = 'a DESC';
CREATE HANDLER h5_${DB} URL '/h5_${DB}' AS SELECT a FROM ${DB}.t UNION ALL SELECT a + 10 FROM ${DB}.t SETTINGS order = 'a', limit = 2;
CREATE HANDLER h6_${DB} URL '/h6_${DB}' AS SELECT a FROM ${DB}.t ORDER BY a FORMAT JSONEachRow SETTINGS limit = 1;
CREATE HANDLER h7_${DB} URL '/h7_${DB}' AS SELECT a FROM ${DB}.t ORDER BY a SETTINGS limit = 1, output_format_tsv_crlf_end_of_line = 1;
CREATE HANDLER h8_${DB} URL '/h8_${DB}' AS SELECT a FROM ${DB}.t ORDER BY a SETTINGS filter = 'a = {n:UInt8}';
CREATE HANDLER h9_${DB} URL '/h9_${DB}' AS SELECT a FROM ${DB}.t ORDER BY a SETTINGS limit = 2;
CREATE HANDLER h10_${DB} URL '/h10_${DB}' METHODS (POST) AS EXECUTE AS ${APP_USER} SELECT a FROM ${DB}.t ORDER BY a SETTINGS filter = 'a = {n:UInt8}';
CREATE HANDLER h11_${DB} URL '/h11_${DB}' AS EXPLAIN SELECT a FROM ${DB}.t SETTINGS filter = 'a = 2';
CREATE HANDLER h14_${DB} URL '/h14_${DB}' METHODS (POST) AS EXECUTE AS ${APP_USER} SELECT a FROM ${DB}.t ORDER BY a FORMAT TSV SETTINGS limit = 2;
CREATE HANDLER h15_${DB} URL '/h15_${DB}' AS EXPLAIN SELECT a FROM ${DB}.t FORMAT TSV SETTINGS filter = 'a = 2';
"

echo "-- limit"
${CLICKHOUSE_CURL} -sS "${BASE}/h1_${DB}"

echo "-- ALTER, filter"
${CLICKHOUSE_CLIENT} -q "ALTER HANDLER h1_${DB} AS SELECT a FROM ${DB}.t ORDER BY a SETTINGS filter = 'a = 2'"
${CLICKHOUSE_CURL} -sS "${BASE}/h1_${DB}"

echo "-- page"
${CLICKHOUSE_CURL} -sS "${BASE}/h3_${DB}"

echo "-- select, filter, order"
${CLICKHOUSE_CURL} -sS "${BASE}/h4_${DB}"

echo "-- UNION with query-level SETTINGS"
${CLICKHOUSE_CURL} -sS "${BASE}/h5_${DB}"

echo "-- FORMAT stays on the outer query"
${CLICKHOUSE_CURL} -sS "${BASE}/h6_${DB}"

echo "-- a query-level setting stays on the outer query"
${CLICKHOUSE_CURL} -sS "${BASE}/h7_${DB}" | tr '\r' '#'

echo "-- query parameter inside a filter"
${CLICKHOUSE_CURL} -sS "${BASE}/h8_${DB}?param_n=2"

echo "-- the request shapes the result of the definition"
${CLICKHOUSE_CURL} -sS "${BASE}/h9_${DB}?limit=5"
${CLICKHOUSE_CURL} -sS "${BASE}/h9_${DB}?filter=a%3E1"

echo "-- EXECUTE AS, query parameter inside a filter"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}/h10_${DB}?param_n=2"

echo "-- EXPLAIN"
${CLICKHOUSE_CURL} -sS "${BASE}/h11_${DB}" | grep -q 'Filter' && echo filtered || echo not filtered
${CLICKHOUSE_CLIENT} -q "SELECT countSubstrings(query, 'SETTINGS') FROM system.handlers WHERE name = 'h11_${DB}'"

echo "-- a second SETTINGS clause"
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h12_${DB} URL '/h12_${DB}' AS SELECT a FROM ${DB}.t ORDER BY a SETTINGS max_threads = 1 FORMAT TSV SETTINGS limit = 1" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted

echo "-- IF NOT EXISTS on an existing handler stays a no-op"
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER IF NOT EXISTS h1_${DB} URL '/h1_${DB}' AS SELECT a FROM ${DB}.t ORDER BY a SETTINGS max_threads = 1 FORMAT TSV SETTINGS limit = 1" && echo noop

echo "-- EXECUTE AS, SETTINGS after FORMAT: the request shapes the result of the definition"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}/h14_${DB}?limit=1"

echo "-- EXPLAIN, SETTINGS after FORMAT"
${CLICKHOUSE_CURL} -sS "${BASE}/h15_${DB}" | grep -q 'Filter' && echo filtered || echo not filtered
${CLICKHOUSE_CLIENT} -q "SELECT countSubstrings(query, 'SETTINGS') FROM system.handlers WHERE name = 'h15_${DB}'"

echo "-- a second SETTINGS clause under EXECUTE AS and EXPLAIN"
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h16_${DB} URL '/h16_${DB}' METHODS (POST) AS EXECUTE AS ${APP_USER} SELECT a FROM ${DB}.t SETTINGS filter = 'a > 1' FORMAT TSV SETTINGS limit = 1" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h17_${DB} URL '/h17_${DB}' AS EXPLAIN SELECT a FROM ${DB}.t SETTINGS filter = 'a > 1' FORMAT TSV SETTINGS limit = 1" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted

echo "-- ALTER with a second SETTINGS clause is rejected, the handler keeps its definition"
${CLICKHOUSE_CLIENT} -q "ALTER HANDLER h9_${DB} AS SELECT a FROM ${DB}.t ORDER BY a SETTINGS max_threads = 1 FORMAT TSV SETTINGS limit = 1" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted
${CLICKHOUSE_CURL} -sS "${BASE}/h9_${DB}"

echo "-- implicit_table_at_top_level for a SELECT without FROM"
${CLICKHOUSE_CLIENT} -q "
CREATE HANDLER h18_${DB} URL '/h18_${DB}' AS SELECT a ORDER BY a SETTINGS implicit_table_at_top_level = '${DB}.t', limit = 2;
CREATE HANDLER h19_${DB} URL '/h19_${DB}' METHODS (POST) AS EXECUTE AS ${APP_USER} SELECT a ORDER BY a FORMAT TSV SETTINGS implicit_table_at_top_level = '${DB}.t', limit = 2;
CREATE HANDLER h20_${DB} URL '/h20_${DB}' METHODS (POST) AS EXECUTE AS ${APP_USER} SELECT a ORDER BY a SETTINGS limit = 2 FORMAT TSV SETTINGS implicit_table_at_top_level = '${DB}.t';
"
${CLICKHOUSE_CURL} -sS "${BASE}/h18_${DB}"
${CLICKHOUSE_CURL} -sS "${BASE}/h18_${DB}?limit=1"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}/h19_${DB}"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}/h19_${DB}?limit=1"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}/h20_${DB}"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}/h20_${DB}?limit=1"
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h21_${DB} URL '/h21_${DB}' AS SELECT a SETTINGS implicit_table_at_top_level = {tb:String}, limit = 2" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted

echo "-- construction settings together with a profile"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS limit = 1"
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h22_${DB} URL '/h22_${DB}' AS SELECT a FROM ${DB}.t ORDER BY a SETTINGS profile = '${PROFILE}', limit = 2" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h23_${DB} URL '/h23_${DB}' METHODS (POST) AS EXECUTE AS ${APP_USER} SELECT a FROM ${DB}.t ORDER BY a FORMAT TSV SETTINGS profile = '${PROFILE}', limit = 2" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h24_${DB} URL '/h24_${DB}' AS SELECT a FROM ${DB}.t ORDER BY a SETTINGS profile = '${PROFILE}'"
${CLICKHOUSE_CURL} -sS "${BASE}/h24_${DB}"

echo "-- construction settings for a SELECT without FROM need a literal implicit_table_at_top_level"
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h25_${DB} URL '/h25_${DB}' AS SELECT count() SETTINGS limit = 1" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted

echo "-- the rejected forms with UNION DISTINCT and INTERSECT, whose normalization drops or moves a SETTINGS clause"
UNION_DISTINCT="SELECT a FROM ${DB}.t UNION DISTINCT SELECT a + 10 FROM ${DB}.t"
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h26_${DB} URL '/h26_${DB}' AS ${UNION_DISTINCT} SETTINGS max_threads = 1 FORMAT TSV SETTINGS limit = 1" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h27_${DB} URL '/h27_${DB}' AS ${UNION_DISTINCT} FORMAT TSV SETTINGS profile = '${PROFILE}', limit = 2" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h28_${DB} URL '/h28_${DB}' AS SELECT count() UNION DISTINCT SELECT count() + 1 FORMAT TSV SETTINGS limit = 1" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER h29_${DB} URL '/h29_${DB}' AS SELECT a FROM ${DB}.t INTERSECT SELECT a FROM ${DB}.t SETTINGS max_threads = 1 FORMAT TSV SETTINGS limit = 1" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted
${CLICKHOUSE_CLIENT} -q "ALTER HANDLER h9_${DB} AS ${UNION_DISTINCT} SETTINGS max_threads = 1 FORMAT TSV SETTINGS limit = 1" 2>&1 \
    | grep -q BAD_ARGUMENTS && echo rejected || echo accepted
${CLICKHOUSE_CURL} -sS "${BASE}/h9_${DB}"
