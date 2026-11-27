#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: uses HTTP handlers with users

# The query of a handler is checked against the user who stores it without a current database. Names that are resolved in
# the current database only when the query runs (a parameterized view, a dictionary, a table in the arguments of a table
# function) are refused, otherwise a request with another `database` would make the handler read another object.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
DB_B="${DB}_b"
BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
U="u_author_${DB}"

http_query() { ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "$1"; }
http_query_as() { ${CLICKHOUSE_CURL} -sS -u "$1:" "${CLICKHOUSE_URL}" --data-binary "$2"; }

cleanup() {
    for h in $(http_query "SELECT name FROM system.handlers WHERE endsWith(name, '_${DB}')"); do
        http_query "DROP HANDLER IF EXISTS \`$h\`"
    done
    http_query "DROP USER IF EXISTS ${U}"
    http_query "DROP DATABASE IF EXISTS ${DB_B}"
}
trap cleanup EXIT
cleanup

# Prints OK, or the name of the error and whether it explains how the query is checked. The current database of the author is ${DB}.
author() {
    local out
    out=$(http_query_as "$U" "$1" 2>&1)
    if [[ "$out" != *"DB::Exception"* ]]; then
        echo OK
    else
        echo "$(echo "$out" | grep -o -E '\([A-Z_]+\)' | tail -n1) $(echo "$out" | grep -o -m1 'The query is checked without a current database')"
    fi
}
# Requests the handler as a user who can read everything, with the other database as the current one.
request() { ${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/$1?database=${DB_B}"; }

$CLICKHOUSE_CLIENT -q "
CREATE DATABASE ${DB_B};
CREATE TABLE ${DB}.t (id UInt64, s String) ENGINE = Memory;
INSERT INTO ${DB}.t VALUES (1, 'public');
CREATE TABLE ${DB_B}.t (id UInt64, s String) ENGINE = Memory;
INSERT INTO ${DB_B}.t VALUES (1, 'secret');
CREATE VIEW ${DB}.v SQL SECURITY INVOKER AS SELECT s FROM ${DB}.t WHERE id = {p:UInt64};
CREATE VIEW ${DB_B}.v SQL SECURITY INVOKER AS SELECT s FROM ${DB_B}.t WHERE id = {p:UInt64};
CREATE DICTIONARY ${DB}.d (id UInt64, s String) PRIMARY KEY id SOURCE(CLICKHOUSE(DB '${DB}' TABLE 't')) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY ${DB_B}.d (id UInt64, s String) PRIMARY KEY id SOURCE(CLICKHOUSE(DB '${DB_B}' TABLE 't')) LAYOUT(FLAT()) LIFETIME(0);
CREATE USER ${U} NOT IDENTIFIED;
GRANT CREATE HANDLER, ALTER HANDLER, CREATE TEMPORARY TABLE ON *.* TO ${U};
GRANT SELECT ON ${DB}.* TO ${U};"

echo "--- 1. a parameterized view"
author "CREATE HANDLER h1_${DB} URL '/${DB}/h1' AS SELECT s FROM v(p = 1)"
author "CREATE HANDLER h1_${DB} URL '/${DB}/h1' AS SELECT s FROM ${DB}.v(p = 1)"
request h1

echo "--- 2. ALTER is checked the same way, and a refused ALTER leaves the handler as it was"
author "ALTER HANDLER h1_${DB} AS SELECT s FROM v(p = 1)"
request h1
author "ALTER HANDLER h1_${DB} URL '/${DB}/h1' AS SELECT concat(s, '!') FROM ${DB}.v(p = 1)"
request h1

echo "--- 3. a dictionary"
author "CREATE HANDLER h3_${DB} URL '/${DB}/h3' AS SELECT s FROM dictionary('d')"
author "CREATE HANDLER h3_${DB} URL '/${DB}/h3' AS SELECT s FROM dictionary('${DB}.d')"
request h3

echo "--- 4. the tables in the query of the view table function are qualified with the database of the author"
author "CREATE HANDLER h4_${DB} URL '/${DB}/h4' AS SELECT s FROM view(SELECT s FROM t)"
request h4

echo "--- 5. a table name is qualified with the database of the author"
author "CREATE HANDLER h5_${DB} URL '/${DB}/h5' AS SELECT s FROM t"
request h5

echo "--- 6. a user with full access is not restricted: the handler runs with the privileges and the database of the request"
http_query "CREATE HANDLER h6_${DB} URL '/${DB}/h6' AS SELECT s FROM v(p = 1)"
${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/h6?database=${DB}"
request h6
