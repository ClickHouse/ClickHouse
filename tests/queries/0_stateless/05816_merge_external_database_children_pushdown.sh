#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every external-database child of a `Merge` table gets the filter pushed down and checked by
# `external_table_strict_query`, also when an earlier child has the same column structure.
# The SQLite tables are STRICT and the columns `Nullable`, so a filter over `id` is pushed down.

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
DB_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}/merge_external.db"
trap 'rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
CREATE TABLE src (id INTEGER, v TEXT) STRICT;
INSERT INTO src VALUES (5, 's5'), (6, 's6'), (7, 's7');
CREATE TABLE src2 (id INTEGER, v TEXT) STRICT;
INSERT INTO src2 VALUES (5, 'q5'), (8, 'q8');
CREATE TABLE loose (id INTEGER, v TEXT);
INSERT INTO loose VALUES (5, 'w5');
"

TABLES="
CREATE TABLE t_l (id Nullable(Int64), v Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_l VALUES (1, 'l1'), (5, 'l5');
CREATE TABLE t_s (id Nullable(Int64), v Nullable(String)) ENGINE = SQLite('${DB_PATH}', 'src');
CREATE TABLE t_s2 (id Nullable(Int64), v Nullable(String)) ENGINE = SQLite('${DB_PATH}', 'src2');
CREATE TABLE t_w (id Nullable(Int64), v Nullable(String)) ENGINE = SQLite('${DB_PATH}', 'loose');
CREATE TABLE t_q (id Nullable(Int64), v Nullable(String)) ENGINE = SQLite('${DB_PATH}', query('SELECT id, v FROM src'));
CREATE TABLE m (id Nullable(Int64), v Nullable(String)) ENGINE = Merge(currentDatabase(), '^t_(l|s)$');
"

echo 'MergeTree and SQLite:'
${CLICKHOUSE_LOCAL} --query="${TABLES}
SELECT id, v FROM m WHERE id = 5 ORDER BY v;
"
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="${TABLES}
SELECT id, v FROM m WHERE id = 5 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT .* FROM `src`.*$'

echo 'Two SQLite tables:'
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="${TABLES}
SELECT id, v FROM merge(currentDatabase(), '^t_s2?$') WHERE id = 5 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT .* FROM `src2?`.*$' | sort

echo 'Strict mode, a filter SQLite cannot evaluate:'
${CLICKHOUSE_LOCAL} --query="${TABLES}
SELECT id, v FROM m WHERE v LIKE 's%' SETTINGS external_table_strict_query = 1;
" 2>&1 | grep -c 'INCORRECT_QUERY'

echo 'Strict mode, a table without STRICT:'
${CLICKHOUSE_LOCAL} --query="${TABLES}
SELECT id, v FROM merge(currentDatabase(), '^t_(s|w)$') WHERE id = 5 SETTINGS external_table_strict_query = 1;
" 2>&1 | grep -c 'INCORRECT_QUERY'

echo 'Strict mode, a query-backed table:'
${CLICKHOUSE_LOCAL} --query="${TABLES}
SELECT id, v FROM merge(currentDatabase(), '^t_(l|q)$') WHERE id = 5 SETTINGS external_table_strict_query = 1;
" 2>&1 | grep -c 'INCORRECT_QUERY'

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE t_l (id Nullable(Int64), v Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_l VALUES (1, 'l1'), (5, 'l5');
CREATE TABLE t_my (id Nullable(Int64), v Nullable(String))
    ENGINE = MySQL('127.0.0.1:${CLICKHOUSE_PORT_MYSQL}', currentDatabase(), 't_l', 'default', '')
    SETTINGS connect_timeout = 100, connection_wait_timeout = 100;
CREATE TABLE m_my (id Nullable(Int64), v Nullable(String)) ENGINE = Merge(currentDatabase(), '^t_(l|my)\$');
CREATE TABLE t_s (id Nullable(Int64), v Nullable(String)) ENGINE = SQLite('${DB_PATH}', 'src');
DROP USER IF EXISTS pg_self_${CLICKHOUSE_DATABASE};
CREATE USER pg_self_${CLICKHOUSE_DATABASE} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO pg_self_${CLICKHOUSE_DATABASE};
CREATE TABLE t_pg (id Nullable(Int64), v Nullable(String))
    ENGINE = PostgreSQL('localhost:${CLICKHOUSE_PORT_POSTGRESQL}', '${CLICKHOUSE_DATABASE}', 't_l', 'pg_self_${CLICKHOUSE_DATABASE}', 'pgpass');
CREATE TABLE m_pg (id Nullable(Int64), v Nullable(String)) ENGINE = Merge(currentDatabase(), '^t_(l|pg)\$');
"

echo 'MergeTree and MySQL:'
${CLICKHOUSE_CLIENT} --send_logs_level=trace --query "
SELECT id, v FROM m_my WHERE id = 5 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT .* FROM `[^`]+`\.`t_l`.*$'

echo 'MergeTree and PostgreSQL:'
${CLICKHOUSE_CLIENT} --send_logs_level=trace --query "
SELECT id, v FROM m_pg WHERE id = 5 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT .* FROM "t_l".*$'

# The receiving side of a serialized query plan has no query tree until it analyzes the read itself.
echo 'Serialized query plan:'
${CLICKHOUSE_CLIENT} --query "
SELECT id, v FROM remote('127.0.0.2', currentDatabase(), t_s) WHERE id = 5
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;
"

echo 'Serialized query plan, strict mode:'
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "
SELECT id, v FROM remote('127.0.0.2', currentDatabase(), t_s) WHERE v LIKE 's%'
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, external_table_strict_query = 1;
" 2>&1 | grep -c 'INCORRECT_QUERY'

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS pg_self_${CLICKHOUSE_DATABASE}"
