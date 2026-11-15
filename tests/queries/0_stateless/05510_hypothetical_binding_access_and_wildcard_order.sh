#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


user="u_05510_${CLICKHOUSE_DATABASE}"
sess="sess_05510_${CLICKHOUSE_DATABASE}"

echo "a type change of a revoked column is denied, not described"
$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE t_access (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100;
    INSERT INTO t_access SELECT number, number FROM numbers(1000);
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT(a, b), SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.t_access TO ${user};
"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&session_id=${sess}&session_timeout=60" \
    --data-binary "CREATE HYPOTHETICAL INDEX i_b ON ${CLICKHOUSE_DATABASE}.t_access (b) TYPE minmax"
$CLICKHOUSE_CLIENT -n -q "
    REVOKE SELECT(b) ON ${CLICKHOUSE_DATABASE}.t_access FROM ${user};
    ALTER TABLE t_access MODIFY COLUMN b UInt32 SETTINGS mutations_sync = 2;
"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&session_id=${sess}&session_timeout=60" \
    --data-binary "EXPLAIN WHATIF SELECT a FROM ${CLICKHOUSE_DATABASE}.t_access WHERE a = 1" 2>&1 | grep -o -m1 -e 'ACCESS_DENIED' -e 'UInt32'
$CLICKHOUSE_CLIENT -q "DROP USER ${user}"

echo "a new order of the columns under * makes a projection with positional GROUP BY not applicable"
$CLICKHOUSE_CLIENT -n -q "
    SET enable_positional_arguments_for_projections = 1, optimize_use_projections = 1;
    CREATE TABLE t_order (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100;
    INSERT INTO t_order SELECT number, number % 10 FROM numbers(1000);
    CREATE HYPOTHETICAL PROJECTION p ON t_order (SELECT *, count() GROUP BY 1, 2);
    SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT b, count() FROM t_order WHERE b = 1 GROUP BY a, b) WHERE explain LIKE '  status:%';
    ALTER TABLE t_order MODIFY COLUMN b UInt64 FIRST;
    SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT b, count() FROM t_order WHERE b = 1 GROUP BY a, b) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';
"
