#!/usr/bin/env bash

# A materialized view hidden behind an `Alias` is expanded by the nested INSERT of the `AliasSink`, so its
# sink is created only during the execution, possibly after a sibling branch writing into the same table has
# made the `Too many parts` decision. With `materialized_views_ignore_errors` the check of such a view, which
# runs with the limits of its `SQL SECURITY` definer, is registered in advance as an isolated check: when it
# fails, the hidden view does not write, and its error is ignored, while the sibling branch with looser
# limits writes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05462_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password SETTINGS parts_to_throw_insert = 1;

    CREATE TABLE t_05462_dst (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS parts_to_throw_insert = 1000;
    SYSTEM STOP MERGES t_05462_dst;

    CREATE TABLE t_05462_hidden (n UInt64) ENGINE = Null;
    CREATE TABLE t_05462_alias ENGINE = Alias('t_05462_hidden');
    CREATE TABLE t_05462_src (n UInt64) ENGINE = Null;

    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05462_hidden TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05462_dst TO $user;

    CREATE MATERIALIZED VIEW t_05462_mv_hidden TO t_05462_dst DEFINER = $user SQL SECURITY DEFINER
        AS SELECT n + 1000 AS n FROM t_05462_hidden;
    CREATE MATERIALIZED VIEW t_05462_mv_direct TO t_05462_dst AS SELECT n FROM t_05462_src;
    CREATE MATERIALIZED VIEW t_05462_mv_alias TO t_05462_alias AS SELECT n FROM t_05462_src;

    INSERT INTO t_05462_dst VALUES (100);
"

# Both orders of the branches: the hidden view never writes, the direct branch always does.
for parallel in 0 1
do
    $CLICKHOUSE_CLIENT --send_logs_level=fatal -q "
        INSERT INTO t_05462_src SETTINGS materialized_views_ignore_errors = 1, parallel_view_processing = $parallel,
            max_insert_threads = 1 VALUES ($parallel + 1);
    "
done

$CLICKHOUSE_CLIENT -q "
    SELECT n FROM t_05462_dst ORDER BY n;

    DROP TABLE t_05462_mv_alias;
    DROP TABLE t_05462_mv_direct;
    DROP TABLE t_05462_mv_hidden SYNC;
    DROP TABLE t_05462_src;
    DROP TABLE t_05462_alias;
    DROP TABLE t_05462_hidden;
    DROP TABLE t_05462_dst;
    DROP USER $user;
"
