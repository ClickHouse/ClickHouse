#!/usr/bin/env bash

# Whether a part lacks a column, which the read then fills from its `DEFAULT`, is decided as the reader decides it, with
# the mutations that are not applied yet. A column dropped and added again is missing in the old parts, which still
# store the dropped data: a column with a non-deterministic default computed from it is not read lazily. A renamed
# column is stored in the old parts under its old name: it is read lazily, and its default is not looked at, so a user
# with only `SELECT` reads it although its default needs a privilege.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="user_05443_${CLICKHOUSE_DATABASE}"
SETTINGS="enable_analyzer = 1, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 1000, optimize_move_to_prewhere = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0, enable_join_runtime_filters = 0, enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --mutations_sync 0 --alter_sync 0 --allow_introspection_functions 1 --query "
    DROP TABLE IF EXISTS l;
    DROP TABLE IF EXISTS t;
    DROP TABLE IF EXISTS r;
    CREATE TABLE r (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO r SELECT number, toString(number) FROM numbers(1000);

    CREATE TABLE l (k UInt64, n UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO l SELECT number, number FROM numbers(1000);
    SYSTEM STOP MERGES l;
    ALTER TABLE l DROP COLUMN n;
    ALTER TABLE l ADD COLUMN n UInt64 DEFAULT rand64();
    ALTER TABLE l ADD COLUMN s String DEFAULT toString(n);

    CREATE TABLE t (k UInt64, s0 String DEFAULT demangle('_Z3foov')) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t SELECT number, concat('s', toString(number)) FROM numbers(1000);
    SYSTEM STOP MERGES t;
    ALTER TABLE t RENAME COLUMN s0 TO s;

    DROP USER IF EXISTS ${USER_NAME};
    CREATE USER ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};
"

echo "dropped and added again"
for enabled in 1 0
do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), countIf(n = toUInt64(s)) FROM (SELECT k, n, s FROM l WHERE n % 2 = 0 ORDER BY k LIMIT 20)
        SETTINGS ${SETTINGS}, query_plan_optimize_lazy_materialization = ${enabled}"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), countIf(n = toUInt64(s))
        FROM (SELECT l.n AS n, l.s AS s, r.payload FROM l JOIN r ON l.k = r.k WHERE l.n % 2 = 0 ORDER BY l.k LIMIT 20)
        SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = ${enabled}"
done

echo "renamed"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "SELECT k, s FROM t ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "
    SELECT t.k, t.s, r.payload FROM t JOIN r ON t.k = r.k ORDER BY t.k DESC LIMIT 3
    SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = 1"
${CLICKHOUSE_CLIENT} --query "
    SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1 SELECT k, s FROM t ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS})
    WHERE explain LIKE '%Lazily read columns%'"

${CLICKHOUSE_CLIENT} --query "
    DROP USER ${USER_NAME};
    DROP TABLE l;
    DROP TABLE t;
    DROP TABLE r;
"
