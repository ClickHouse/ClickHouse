#!/usr/bin/env bash

# A column that every part stores is read lazily whatever its `DEFAULT` is, and reading it does not need what the
# default needs: here a user with only `SELECT` reads a column with `DEFAULT demangle(...)`, which needs a privilege and
# `allow_introspection_functions`. The plans, which show the column read lazily, are made without the setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="user_05442_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --allow_introspection_functions 1 --query "
    DROP TABLE IF EXISTS t;
    DROP TABLE IF EXISTS r;
    CREATE TABLE t (k UInt64, s String DEFAULT demangle('_Z3foov')) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE r (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t SELECT number, concat('s', toString(number)) FROM numbers(1000);
    INSERT INTO r SELECT number, toString(number) FROM numbers(1000);
    DROP USER IF EXISTS ${USER_NAME};
    CREATE USER ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};
"

SETTINGS="enable_analyzer = 1, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 1000, optimize_move_to_prewhere = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0, enable_join_runtime_filters = 0, enable_parallel_replicas = 0"

echo "order by"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "SELECT k, s FROM t ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "
    SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1 SELECT k, s FROM t ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS})
    WHERE explain LIKE '%Lazily read columns%'"

echo "join"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "
    SELECT t.k, t.s, r.payload FROM t JOIN r ON t.k = r.k ORDER BY t.k DESC LIMIT 3
    SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = 1"
${CLICKHOUSE_CLIENT} --query "
    SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
        SELECT t.k, t.s, r.payload FROM t JOIN r ON t.k = r.k ORDER BY t.k DESC LIMIT 3
        SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = 1)
    WHERE explain LIKE '%Lazily read columns%'"

${CLICKHOUSE_CLIENT} --query "
    DROP USER ${USER_NAME};
    DROP TABLE t;
    DROP TABLE r;
"
