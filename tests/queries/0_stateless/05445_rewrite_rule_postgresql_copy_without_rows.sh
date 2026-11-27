#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: rewrite rules are global server state
# Tag no-fasttest: Requires postgresql-client

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The result of `COPY (query) TO STDOUT` is streamed to the client, so a rewrite rule must not turn
# the query into a statement without a result. Such a rewrite is rejected before it is executed,
# and the connection stays usable.

PG_USER="postgresql_user_05445_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
DROP RULE IF EXISTS rule_05445;
CREATE RULE rule_05445 AS (SELECT 123456789) REWRITE TO (SET max_threads = 1);
" < /dev/null

psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only 2>&1 <<'EOF' | grep -o -E 'does not return rows|^[A-Z0-9]+$'
SET query_rules = 'rule_05445';
COPY (SELECT 123456789) TO STDOUT;
SELECT 1;
EOF

${CLICKHOUSE_CLIENT} -q "
DROP RULE rule_05445;
DROP USER ${PG_USER};
" < /dev/null
