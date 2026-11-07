#!/usr/bin/env bash
# The `Too many parts` checks of the tables behind a forwarding storage run before the nested INSERT checks
# the access rights. Every insertion boundary on the way to such a table is authorized with the context the
# write arrives with, before the dependent views of the boundary are followed with the `SQL SECURITY`
# contexts of the views. Here the user may insert into a `Distributed` table over a private `Null` table,
# and a definer view owned by another user writes from the private table into a destination that is
# already at its part limit: the user must receive `ACCESS_DENIED` for the private table rather than a
# `TOO_MANY_PARTS` error telling the number and the sizes of the parts of the destination.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05459_${CLICKHOUSE_DATABASE}"
definer="definer_05459_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $user;
    DROP USER IF EXISTS $definer;
    DROP TABLE IF EXISTS t_05459_mv;
    DROP TABLE IF EXISTS t_05459_dist;
    DROP TABLE IF EXISTS t_05459_private;
    DROP TABLE IF EXISTS t_05459_dst;

    CREATE TABLE t_05459_dst (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS parts_to_throw_insert = 1;
    CREATE TABLE t_05459_private (n UInt64) ENGINE = Null;
    CREATE TABLE t_05459_dist (n UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05459_private);
    INSERT INTO t_05459_dst VALUES (100);

    CREATE USER $definer IDENTIFIED WITH no_password;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05459_private TO $definer;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05459_dst TO $definer;
    CREATE MATERIALIZED VIEW t_05459_mv TO t_05459_dst DEFINER = $definer SQL SECURITY DEFINER
        AS SELECT n FROM t_05459_private;

    CREATE USER $user IDENTIFIED WITH no_password;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05459_dist TO $user;
"

for foreground in 0 1; do
    $CLICKHOUSE_CLIENT --user "$user" -q "
        INSERT INTO t_05459_dist SETTINGS distributed_foreground_insert = $foreground, prefer_localhost_replica = 1 VALUES (1)
    " 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|TOO_MANY_PARTS'
done

# With the right to insert into the private table the limit of the destination applies.
$CLICKHOUSE_CLIENT -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05459_private TO $user"

for foreground in 0 1; do
    $CLICKHOUSE_CLIENT --user "$user" -q "
        INSERT INTO t_05459_dist SETTINGS distributed_foreground_insert = $foreground, prefer_localhost_replica = 1 VALUES (1)
    " 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|TOO_MANY_PARTS'
done

$CLICKHOUSE_CLIENT -q "
    SELECT n FROM t_05459_dst ORDER BY n;
    DROP TABLE t_05459_mv;
    DROP TABLE t_05459_dist;
    DROP TABLE t_05459_private;
    DROP TABLE t_05459_dst;
    DROP USER $user;
    DROP USER $definer;
"
