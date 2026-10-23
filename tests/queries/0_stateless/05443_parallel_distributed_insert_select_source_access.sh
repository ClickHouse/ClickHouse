#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE secret_local (id UInt64, secret String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE copy_local (id UInt64, secret String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO secret_local VALUES (1, 'secret');

    CREATE TABLE src_dist (id UInt64, secret String) ENGINE = Distributed(test_shard_localhost, currentDatabase(), secret_local, id);
    CREATE TABLE dst_dist (id UInt64, secret String) ENGINE = Distributed(test_shard_localhost, currentDatabase(), copy_local, id);

    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password;
    GRANT SELECT, INSERT ON $CLICKHOUSE_DATABASE.dst_dist TO $user;
"

# The cluster has no interserver secret, so the shard runs the forwarded query under the credentials
# from the cluster configuration rather than as the caller.
client=($CLICKHOUSE_CLIENT --user "$user" --prefer_localhost_replica 0)

for setting in 0 1 2; do
    echo -n "parallel_distributed_insert_select=$setting "
    "${client[@]}" --parallel_distributed_insert_select "$setting" -q "INSERT INTO dst_dist SELECT * FROM src_dist" |&
        grep -o -m1 'ACCESS_DENIED' || echo 'NOT DENIED'
done

# Read the destination directly: resolving `dst_dist` also authorizes `copy_local` once the shard
# query is planned on the initiator, which depends on the distributed plan settings.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM copy_local"

# With the grant in place the optimized path must still work.
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.src_dist TO $user"
"${client[@]}" --parallel_distributed_insert_select 2 -q "INSERT INTO dst_dist SELECT * FROM src_dist"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM copy_local"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
