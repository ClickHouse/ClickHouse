#!/usr/bin/env bash
# The `Too many parts` checks of the tables behind a forwarding storage run before the nested INSERT into
# such a table checks the access rights. A user who may not insert into the table must receive
# `ACCESS_DENIED` rather than a `TOO_MANY_PARTS` error telling the number and the sizes of its parts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05454_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $user;
    DROP TABLE IF EXISTS t_05454_dist;
    DROP TABLE IF EXISTS t_05454_local;

    CREATE TABLE t_05454_local (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS parts_to_throw_insert = 1;
    CREATE TABLE t_05454_dist (n UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05454_local);
    INSERT INTO t_05454_local VALUES (100);

    CREATE USER $user IDENTIFIED WITH no_password;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05454_dist TO $user;
"

for foreground in 0 1; do
    $CLICKHOUSE_CLIENT --user "$user" -q "
        INSERT INTO t_05454_dist SETTINGS distributed_foreground_insert = $foreground, prefer_localhost_replica = 1 VALUES (1)
    " 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|TOO_MANY_PARTS'
done

# With the right to insert into a column of the local table the limit of the table applies.
$CLICKHOUSE_CLIENT -q "GRANT INSERT(n) ON ${CLICKHOUSE_DATABASE}.t_05454_local TO $user"

for foreground in 0 1; do
    $CLICKHOUSE_CLIENT --user "$user" -q "
        INSERT INTO t_05454_dist SETTINGS distributed_foreground_insert = $foreground, prefer_localhost_replica = 1 VALUES (1)
    " 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|TOO_MANY_PARTS'
done

$CLICKHOUSE_CLIENT -q "
    SELECT n FROM t_05454_local ORDER BY n;
    DROP USER $user;
    DROP TABLE t_05454_dist;
    DROP TABLE t_05454_local;
"
