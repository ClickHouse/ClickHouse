#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An external child of a `Merge` table on the non-preserving side of an outer join must not apply
# the filter before the join: the rows the join pads with defaults have to reach the filter.

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
DB_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}/merge_outer_join.db"
trap 'rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
CREATE TABLE p (k INTEGER NOT NULL, id INTEGER NOT NULL, description TEXT) STRICT;
INSERT INTO p VALUES (0, 0, NULL), (0, 1, 'one');
"

TABLES="
CREATE TABLE orders (k Int64, id Int64) ENGINE = Memory;
INSERT INTO orders VALUES (0, 1), (0, 2);
CREATE TABLE p_ext (k Int64, id Int64, description Nullable(String)) ENGINE = SQLite('${DB_PATH}', 'p');
CREATE TABLE a_local (k Int64, id Int64, description Nullable(String)) ENGINE = Memory;
"

${CLICKHOUSE_LOCAL} --query="${TABLES}
SELECT 'LEFT JOIN';
SELECT o.id FROM orders AS o LEFT JOIN merge(currentDatabase(), '^p_ext$') AS p ON o.id = p.id
WHERE p.description IS NULL ORDER BY o.id;
SELECT 'LEFT JOIN, direct read';
SELECT o.id FROM orders AS o LEFT JOIN p_ext AS p ON o.id = p.id
WHERE p.description IS NULL ORDER BY o.id;
SELECT 'RIGHT JOIN';
SELECT o.id FROM merge(currentDatabase(), '^p_ext$') AS p RIGHT JOIN orders AS o ON o.id = p.id
WHERE p.description IS NULL ORDER BY o.id;
SELECT 'FULL JOIN';
SELECT o.id FROM orders AS o FULL JOIN merge(currentDatabase(), '^p_ext$') AS p ON o.id = p.id
WHERE p.description IS NULL ORDER BY o.id SETTINGS join_use_nulls = 0;
SELECT 'LEFT JOIN under CROSS JOIN';
SELECT o.id FROM orders AS o LEFT JOIN merge(currentDatabase(), '^p_ext$') AS p ON o.id = p.id CROSS JOIN (SELECT 1) AS x
WHERE p.description IS NULL ORDER BY o.id;
SELECT 'ASOF JOIN';
SELECT count() FROM orders AS o ASOF JOIN merge(currentDatabase(), '^p_ext$') AS p ON o.k = p.k AND o.id >= p.id
WHERE p.description IS NULL SETTINGS join_algorithm = 'hash';
SELECT 'LEFT JOIN, a local table first';
SELECT o.id FROM orders AS o LEFT JOIN merge(currentDatabase(), '^(a_local|p_ext)$') AS p ON o.id = p.id
WHERE p.description IS NULL ORDER BY o.id;
SELECT 'Strict mode, direct read';
SELECT o.id FROM orders AS o LEFT JOIN p_ext AS p ON o.id = p.id
WHERE p.description LIKE 'o%' ORDER BY o.id SETTINGS external_table_strict_query = 1;
"

echo 'Strict mode'
${CLICKHOUSE_LOCAL} --query="${TABLES}
SELECT o.id FROM orders AS o LEFT JOIN merge(currentDatabase(), '^p_ext$') AS p ON o.id = p.id
WHERE p.description LIKE 'o%' ORDER BY o.id SETTINGS external_table_strict_query = 1;
" 2>&1

echo 'Pushed down on the preserving side:'
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="${TABLES}
SELECT p.id FROM merge(currentDatabase(), '^p_ext$') AS p LEFT JOIN orders AS o ON o.id = p.id
WHERE p.description = 'one' FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT .* FROM `p`.*$'

echo 'Pushed down under INNER JOIN:'
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="${TABLES}
SELECT o.id FROM orders AS o INNER JOIN merge(currentDatabase(), '^p_ext$') AS p ON o.id = p.id
WHERE p.description = 'one' FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT .* FROM `p`.*$'

echo 'Not pushed down on the non-preserving side:'
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="${TABLES}
SELECT o.id FROM orders AS o LEFT JOIN merge(currentDatabase(), '^p_ext$') AS p ON o.id = p.id
WHERE p.description IS NULL FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT .* FROM `p`.*$'
