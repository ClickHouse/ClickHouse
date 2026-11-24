#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite

# `reattach_tables_before_query_execution` must keep a query that stops before reading its sources side-effect
# free: a `CREATE TABLE ... (explicit columns) AS SELECT` over a taken destination name, a populating `CREATE`
# without `INSERT` on its destination, and scalar functions that check a grant of their own.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_es_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_es_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_es_src (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_es_src VALUES (1)"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_es_dst (a UInt64) ENGINE = MergeTree ORDER BY a"

# 1. With an explicit column list the populating `SELECT` is not analyzed before the taken-name check, so the
# statement no-ops (`IF NOT EXISTS`) or throws `TABLE_ALREADY_EXISTS` without reading the source.
check_if_not_detached "CREATE TABLE IF NOT EXISTS t_reattach_es_dst (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT a FROM t_reattach_es_src" "t_reattach_es_src"
check_fails_kind_without_detach "CREATE TABLE t_reattach_es_dst (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT a FROM t_reattach_es_src" "t_reattach_es_src" "TABLE_ALREADY_EXISTS"
# The inferred-schema form analyzes the `SELECT` first, so its source stays eligible.
check_if_detached "CREATE TABLE IF NOT EXISTS t_reattach_es_dst ENGINE = MergeTree ORDER BY a AS SELECT a FROM t_reattach_es_src" "t_reattach_es_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_es_dst"

# 2. A populating `CREATE` needs `INSERT` on its destination.
ES_USER="user_reattach_es_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${ES_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${ES_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${ES_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_es_dst TO ${ES_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_es_src TO ${ES_USER}"

CREATE_QUERY="CREATE TABLE t_reattach_es_dst (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT a FROM t_reattach_es_src"
REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${ES_USER}" --reattach_tables_before_query_execution=1 --query "${CREATE_QUERY}" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_es_src"; then
    echo "FAIL (source was detached for an access-rejected query)"
else
    echo "OK"
fi

# The granted control detaches the source.
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_reattach_es_dst TO ${ES_USER}"
REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${ES_USER}" --reattach_tables_before_query_execution=1 --query "${CREATE_QUERY}" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_es_src"; then
    echo "OK"
else
    echo "FAIL (source was not detached)"
fi
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_reattach_es_dst"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${ES_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_es_dst"

# 3. Scalar functions that check a grant of their own when they are created are not seen by the preflights,
# so a query calling one is skipped; the plain read is the control.
check_if_detached "SELECT a FROM t_reattach_es_src" "t_reattach_es_src"
check_if_not_detached "SELECT a, getServerSetting('mark_cache_policy') != '' FROM t_reattach_es_src FORMAT Null" "t_reattach_es_src"
check_if_not_detached "SELECT a, getMergeTreeSetting('index_granularity') FROM t_reattach_es_src FORMAT Null" "t_reattach_es_src"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_es_src"
