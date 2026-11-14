#!/usr/bin/env bash

# One hidden materialized view reached by two branches of the query with different `Too many parts` limits:
# two views of the source write through two `Alias` tables of the same `Null` table, one of them with the
# limits of its `SQL SECURITY DEFINER`, and the view of that `Null` table (`SQL SECURITY NONE`) keeps the
# incoming settings. Under `materialized_views_ignore_errors` the check of the hidden view is registered in
# advance once per branch, as an isolated check, and the sink the nested INSERT creates for the view observes
# the outcome of the check of its own branch: the strict branch does not write, the loose one does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05508_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password SETTINGS parts_to_throw_insert = 1;

    CREATE TABLE t_05508_dst (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS parts_to_throw_insert = 1000;
    SYSTEM STOP MERGES t_05508_dst;

    CREATE TABLE t_05508_hidden (n UInt64) ENGINE = Null;
    CREATE TABLE t_05508_alias_strict ENGINE = Alias('t_05508_hidden');
    CREATE TABLE t_05508_alias_loose ENGINE = Alias('t_05508_hidden');
    CREATE TABLE t_05508_src (n UInt64) ENGINE = Null;

    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO $user;

    CREATE MATERIALIZED VIEW t_05508_mv_hidden TO t_05508_dst SQL SECURITY NONE
        AS SELECT n + 1000 AS n FROM t_05508_hidden;
    CREATE MATERIALIZED VIEW t_05508_mv_direct TO t_05508_dst AS SELECT n FROM t_05508_src;
    CREATE MATERIALIZED VIEW t_05508_mv_strict TO t_05508_alias_strict DEFINER = $user SQL SECURITY DEFINER
        AS SELECT n FROM t_05508_src;
    CREATE MATERIALIZED VIEW t_05508_mv_loose TO t_05508_alias_loose AS SELECT n + 10 AS n FROM t_05508_src;

    INSERT INTO t_05508_dst VALUES (100);
"

# Both orders of the branches: the strict branch of the hidden view never writes, the loose one always does.
for parallel in 0 1
do
    $CLICKHOUSE_CLIENT --send_logs_level=fatal -q "
        INSERT INTO t_05508_src SETTINGS materialized_views_ignore_errors = 1, parallel_view_processing = $parallel,
            max_insert_threads = 1 VALUES ($parallel + 1);
    "
done

$CLICKHOUSE_CLIENT -q "
    SELECT n FROM t_05508_dst ORDER BY n;

    DROP TABLE t_05508_mv_loose;
    DROP TABLE t_05508_mv_strict;
    DROP TABLE t_05508_mv_direct;
    DROP TABLE t_05508_mv_hidden SYNC;
    DROP TABLE t_05508_src;
    DROP TABLE t_05508_alias_loose;
    DROP TABLE t_05508_alias_strict;
    DROP TABLE t_05508_hidden;
    DROP TABLE t_05508_dst;
    DROP USER $user;
"
