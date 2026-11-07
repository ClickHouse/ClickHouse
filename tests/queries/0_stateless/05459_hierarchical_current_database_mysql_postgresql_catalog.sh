#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires mysql and postgresql clients

# A hierarchical current database (`USE db.ns`) denotes the tables `ns.*` of the database `db` and the
# databases `db.ns.*`. The catalog emulation of the MySQL (`SHOW TABLE STATUS`) and PostgreSQL
# (`pg_table_is_visible`, used by psql's `\d`) protocols must see the same tables as `SHOW TABLES`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_USER="postgresql_user_05459_${CLICKHOUSE_DATABASE}"
NESTED_DATABASE="${CLICKHOUSE_DATABASE}.ns.sub"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
DROP DATABASE IF EXISTS \`${NESTED_DATABASE}\`;

CREATE DATABASE \`${NESTED_DATABASE}\`;
CREATE TABLE ${CLICKHOUSE_DATABASE}.\`ns.orders\` (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${CLICKHOUSE_DATABASE}.other (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE \`${NESTED_DATABASE}\`.items (x UInt32) ENGINE = MergeTree ORDER BY x;

CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
GRANT SELECT ON \`${NESTED_DATABASE}\`.* TO ${PG_USER};
"

echo '--- SHOW TABLES'
${CLICKHOUSE_CLIENT} -q "USE ${CLICKHOUSE_DATABASE}.ns; SHOW TABLES"

echo '--- MySQL SHOW TABLE STATUS'
function run_mysql()
{
    ${MYSQL_CLIENT} --database "${CLICKHOUSE_DATABASE}.ns" --batch --skip-column-names --execute "$1" 2>/dev/null | cut -f1,2 | sort
}
run_mysql "SHOW TABLE STATUS LIKE '%'"
run_mysql "SHOW TABLE STATUS LIKE 'orders'"
run_mysql "SHOW TABLE STATUS LIKE 'other'"

echo '--- PostgreSQL pg_table_is_visible'
psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}.ns" --user "${PG_USER}" \
    --no-psqlrc --tuples-only --no-align 2>&1 <<SQL
SELECT c.relname, pg_catalog.pg_table_is_visible(c.oid)
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname IN ('${CLICKHOUSE_DATABASE}', '${NESTED_DATABASE}')
ORDER BY c.relname;
SQL

${CLICKHOUSE_CLIENT} -q "
DROP DATABASE \`${NESTED_DATABASE}\`;
DROP USER ${PG_USER};
"
