#!/usr/bin/env bash

# Lazy materialization decides whether a part lacks a column, which the read then fills from its `DEFAULT`, as the reader
# decides it: with the mutations that are not applied yet, and with the parts the primary key leaves out. Looking at a
# default does not need the privileges its functions need: a user with only `SELECT` reads columns with
# `DEFAULT demangle(...)`, which needs a privilege and `allow_introspection_functions`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="user_05840_${CLICKHOUSE_DATABASE}"
SETTINGS="enable_analyzer = 1, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 1000, optimize_move_to_prewhere = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0, enable_join_runtime_filters = 0, enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --mutations_sync 0 --alter_sync 0 --allow_introspection_functions 1 --query "
    DROP TABLE IF EXISTS stored;
    DROP TABLE IF EXISTS dropped;
    DROP TABLE IF EXISTS renamed;
    DROP TABLE IF EXISTS nullable;
    DROP TABLE IF EXISTS skipped;
    DROP TABLE IF EXISTS r;
    CREATE TABLE r (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO r SELECT number, toString(number) FROM numbers(2000);

    -- Every part stores the column.
    CREATE TABLE stored (k UInt64, s String DEFAULT demangle('_Z3foov')) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO stored SELECT number, concat('s', toString(number)) FROM numbers(1000);

    -- The old part still stores the data of the dropped column, which the reader does not read.
    CREATE TABLE dropped (k UInt64, n UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO dropped SELECT number, number FROM numbers(1000);
    SYSTEM STOP MERGES dropped;
    ALTER TABLE dropped DROP COLUMN n;
    ALTER TABLE dropped ADD COLUMN n UInt64 DEFAULT rand64();
    ALTER TABLE dropped ADD COLUMN s String DEFAULT toString(n);

    -- The old part stores the renamed column under its old name.
    CREATE TABLE renamed (k UInt64, s0 String DEFAULT demangle('_Z3foov')) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO renamed SELECT number, concat('s', toString(number)) FROM numbers(1000);
    SYSTEM STOP MERGES renamed;
    ALTER TABLE renamed RENAME COLUMN s0 TO s;

    -- The old part stores the column as \`Nullable\`, and the read fills its \`NULL\` values from the default.
    CREATE TABLE nullable (k UInt64, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO nullable SELECT number, NULL FROM numbers(1000);
    SYSTEM STOP MERGES nullable;
    ALTER TABLE nullable MODIFY COLUMN n UInt64 DEFAULT rand64();
    ALTER TABLE nullable ADD COLUMN s String DEFAULT toString(n);

    -- The old part lacks the column, and the primary key leaves it out.
    CREATE TABLE skipped (k UInt64) ENGINE = MergeTree ORDER BY k;
    SYSTEM STOP MERGES skipped;
    INSERT INTO skipped SELECT number FROM numbers(1000);
    ALTER TABLE skipped ADD COLUMN s String DEFAULT demangle('_Z3foov');
    INSERT INTO skipped SELECT number, concat('s', toString(number)) FROM numbers(1000, 1000);

    DROP USER IF EXISTS ${USER_NAME};
    CREATE USER ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};
"

function lazily_read()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1 $1)
        WHERE explain LIKE '%Lazily read columns%'"
}

echo "stored"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "SELECT k, s FROM stored ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}"
lazily_read "SELECT k, s FROM stored ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "
    SELECT stored.k, stored.s, r.payload FROM stored JOIN r ON stored.k = r.k ORDER BY stored.k DESC LIMIT 3
    SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = 1"
lazily_read "SELECT stored.k, stored.s, r.payload FROM stored JOIN r ON stored.k = r.k ORDER BY stored.k DESC LIMIT 3
    SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = 1"

for table in dropped nullable
do
    echo "${table}"
    for enabled in 1 0
    do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT count(), countIf(n = toUInt64(s)) FROM (SELECT k, n, s FROM ${table} WHERE n % 2 = 0 ORDER BY k LIMIT 20)
            SETTINGS ${SETTINGS}, query_plan_optimize_lazy_materialization = ${enabled}"
        ${CLICKHOUSE_CLIENT} --query "
            SELECT count(), countIf(n = toUInt64(s))
            FROM (SELECT l.n AS n, l.s AS s, r.payload FROM ${table} AS l JOIN r ON l.k = r.k WHERE l.n % 2 = 0 ORDER BY l.k LIMIT 20)
            SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = ${enabled}"
    done
done

echo "renamed"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "SELECT k, s FROM renamed ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "
    SELECT renamed.k, renamed.s, r.payload FROM renamed JOIN r ON renamed.k = r.k ORDER BY renamed.k DESC LIMIT 3
    SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = 1"
lazily_read "SELECT k, s FROM renamed ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}"

echo "skipped"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "SELECT k, s FROM skipped WHERE k >= 1000 ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}, optimize_read_in_order = 0"
lazily_read "SELECT k, s FROM skipped WHERE k >= 1000 ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}, optimize_read_in_order = 0"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --query "
    SELECT skipped.k, skipped.s, r.payload FROM skipped JOIN r ON skipped.k = r.k WHERE skipped.k >= 1000 ORDER BY skipped.k DESC LIMIT 3
    SETTINGS ${SETTINGS}, optimize_read_in_order = 0, query_plan_lazy_materialization_for_join = 1"

${CLICKHOUSE_CLIENT} --query "
    DROP USER ${USER_NAME};
    DROP TABLE stored;
    DROP TABLE dropped;
    DROP TABLE renamed;
    DROP TABLE nullable;
    DROP TABLE skipped;
    DROP TABLE r;
"
