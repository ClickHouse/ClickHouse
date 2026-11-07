#!/usr/bin/env bash

# The sinks of one INSERT writing into the same table share one `Too many parts` decision. A table created
# `AS remote(...)` over a local table forwards the write through the nested INSERT of its `Distributed` sink,
# which creates the sink of the local table only during the execution, possibly after a sibling branch writing
# into the same table directly has made the decision. The check of the hidden local write, which runs with the
# limits of the `SQL SECURITY` definer of its view, must still be enforced, whichever branch starts first.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05507_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password SETTINGS parts_to_throw_insert = 1;

    CREATE TABLE t_05507_dst (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS parts_to_throw_insert = 1000;
    SYSTEM STOP MERGES t_05507_dst;

    CREATE TABLE t_05507_forwarded AS remote('127.0.0.1', '${CLICKHOUSE_DATABASE}', 't_05507_dst');
    CREATE TABLE t_05507_src_1 (n UInt64) ENGINE = Null;
    CREATE TABLE t_05507_src_2 (n UInt64) ENGINE = Null;

    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05507_src_1 TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05507_src_2 TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05507_forwarded TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05507_dst TO $user;

    -- Both orders of the branches: through the proxy first, and directly first.
    CREATE MATERIALIZED VIEW t_05507_mv_a TO t_05507_forwarded DEFINER = $user SQL SECURITY DEFINER
        AS SELECT n + 1000 AS n FROM t_05507_src_1;
    CREATE MATERIALIZED VIEW t_05507_mv_b TO t_05507_dst AS SELECT n FROM t_05507_src_1;
    CREATE MATERIALIZED VIEW t_05507_mv_c TO t_05507_dst AS SELECT n FROM t_05507_src_2;
    CREATE MATERIALIZED VIEW t_05507_mv_d TO t_05507_forwarded DEFINER = $user SQL SECURITY DEFINER
        AS SELECT n + 1000 AS n FROM t_05507_src_2;

    INSERT INTO t_05507_dst VALUES (100);
"

# The table already has one part, which is too many for the definer of the views writing through the proxy.
for parallel in 0 1
do
    for src in 1 2
    do
        $CLICKHOUSE_CLIENT -q "
            INSERT INTO t_05507_src_$src SETTINGS prefer_localhost_replica = 1, parallel_view_processing = $parallel,
                max_insert_threads = 1 VALUES ($parallel * 2 + $src);
        " 2>&1 | grep -o -m1 'TOO_MANY_PARTS' || echo "accepted"
    done
done

$CLICKHOUSE_CLIENT -q "
    -- Nothing has been written through the proxy.
    SELECT count() FROM t_05507_dst WHERE n >= 1000;

    DROP TABLE t_05507_mv_a;
    DROP TABLE t_05507_mv_b;
    DROP TABLE t_05507_mv_c;
    DROP TABLE t_05507_mv_d SYNC;
    DROP TABLE t_05507_src_1;
    DROP TABLE t_05507_src_2;
    DROP TABLE t_05507_forwarded;
    DROP TABLE t_05507_dst;
    DROP USER $user;
"
