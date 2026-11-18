#!/usr/bin/env bash

# Whether a column is read lazily is decided before the analysis of ranges, so the defaults of the columns that the
# parts left out by the primary key lack are looked at as well. Looking at a default does not need the privileges its
# functions need: here a user with only `SELECT` reads a column with `DEFAULT demangle(...)`, which the old part lacks,
# and which is read lazily. The primary key leaves the old part out.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="user_05830_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --allow_introspection_functions 1 --query "
    DROP TABLE IF EXISTS t;
    DROP TABLE IF EXISTS r;
    CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
    SYSTEM STOP MERGES t;
    INSERT INTO t SELECT number FROM numbers(1000);
    ALTER TABLE t ADD COLUMN s String DEFAULT demangle('_Z3foov');
    INSERT INTO t SELECT number, concat('s', toString(number)) FROM numbers(1000, 1000);
    CREATE TABLE r (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO r SELECT number, toString(number) FROM numbers(2000);
    DROP USER IF EXISTS ${USER_NAME};
    CREATE USER ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};
"

SETTINGS="enable_analyzer = 1, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 1000, optimize_read_in_order = 0, optimize_move_to_prewhere = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0, enable_join_runtime_filters = 0, enable_parallel_replicas = 0"

echo "order by"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "SELECT k, s FROM t WHERE k >= 1000 ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "
    SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
        SELECT k, s FROM t WHERE k >= 1000 ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS})
    WHERE explain LIKE '%Lazily read columns%'"

echo "join"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "
    SELECT t.k, t.s, r.payload FROM t JOIN r ON t.k = r.k WHERE t.k >= 1000 ORDER BY t.k DESC LIMIT 3
    SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = 1"

${CLICKHOUSE_CLIENT} --query "
    DROP USER ${USER_NAME};
    DROP TABLE t;
    DROP TABLE r;
"
