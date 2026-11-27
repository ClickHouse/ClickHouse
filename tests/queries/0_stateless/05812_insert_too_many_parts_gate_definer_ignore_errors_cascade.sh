#!/usr/bin/env bash

# `materialized_views_ignore_errors` of a cascade of views is that of the INSERT expanding the cascade: the
# `InsertDependenciesBuilder` samples it once, from the context of the (nested) INSERT. The deferred checks
# registered in advance for the views hidden behind an `Alias` must follow the same policy, and not the settings
# of the `SQL SECURITY` definer of a view on the way. Here the definer ignores the errors of the views and has
# strict limits, while the invoker does not ignore them and has loose limits: the check of the final view, which
# runs with the definer's limits, is a regular one and rejects the whole INSERT, in both orders of the branches.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05812_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password SETTINGS parts_to_throw_insert = 1, materialized_views_ignore_errors = 1;

    CREATE TABLE t_05812_dst (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS parts_to_throw_insert = 1000;
    SYSTEM STOP MERGES t_05812_dst;

    CREATE TABLE t_05812_mid (n UInt64) ENGINE = Null;
    CREATE TABLE t_05812_hidden (n UInt64) ENGINE = Null;
    CREATE TABLE t_05812_alias ENGINE = Alias('t_05812_hidden');
    CREATE TABLE t_05812_src (n UInt64) ENGINE = Null;

    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05812_hidden TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05812_mid TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05812_mid TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05812_dst TO $user;

    CREATE MATERIALIZED VIEW t_05812_mv_final TO t_05812_dst SQL SECURITY NONE AS SELECT n + 2000 AS n FROM t_05812_mid;
    CREATE MATERIALIZED VIEW t_05812_mv_definer TO t_05812_mid DEFINER = $user SQL SECURITY DEFINER
        AS SELECT n FROM t_05812_hidden;
    CREATE MATERIALIZED VIEW t_05812_mv_direct TO t_05812_dst AS SELECT n FROM t_05812_src;
    CREATE MATERIALIZED VIEW t_05812_mv_alias TO t_05812_alias AS SELECT n FROM t_05812_src;

    INSERT INTO t_05812_dst VALUES (100);
"

for parallel in 0 1
do
    $CLICKHOUSE_CLIENT --send_logs_level=fatal -q "
        INSERT INTO t_05812_src SETTINGS materialized_views_ignore_errors = 0, parallel_view_processing = $parallel,
            max_insert_threads = 1 VALUES ($parallel + 1);
    " 2>&1 | grep -o -m1 'TOO_MANY_PARTS'
done

$CLICKHOUSE_CLIENT -q "
    SELECT n FROM t_05812_dst ORDER BY n;

    DROP TABLE t_05812_mv_alias;
    DROP TABLE t_05812_mv_direct;
    DROP TABLE t_05812_mv_definer SYNC;
    DROP TABLE t_05812_mv_final;
    DROP TABLE t_05812_src;
    DROP TABLE t_05812_alias;
    DROP TABLE t_05812_hidden;
    DROP TABLE t_05812_mid;
    DROP TABLE t_05812_dst;
    DROP USER $user;
"
