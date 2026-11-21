#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: rewrite rules are global server state
# Tag no-fasttest: Requires postgresql-client

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The result of `COPY (query) TO STDOUT` is streamed to the client, so a rewrite rule must not turn
# the query into a statement without a result. A rewrite into a read-only statement that does
# return rows, such as `SHOW TABLES`, is allowed.

PG_USER="postgresql_user_05508_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SHOW TABLES ON system.one TO ${PG_USER};
DROP RULE IF EXISTS rule_05508;
CREATE RULE rule_05508 AS (SELECT 123456789) REWRITE TO (SHOW TABLES FROM system LIKE 'one');
" < /dev/null

psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only 2>&1 <<'EOF' | grep -o -E 'does not return rows|^[A-Za-z0-9]+$'
SET query_rules = 'rule_05508';
COPY (SELECT 123456789) TO STDOUT;
SELECT 1;
EOF

${CLICKHOUSE_CLIENT} -q "
DROP RULE rule_05508;
DROP USER ${PG_USER};
" < /dev/null
