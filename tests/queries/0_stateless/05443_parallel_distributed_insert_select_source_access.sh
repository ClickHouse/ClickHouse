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

# A row policy on the source table cannot be applied by the optimized rewrite (it ships the query
# to the shards, where the policy on the Distributed table never applies), so it must fall back to
# the regular path, which rejects a row policy on a Distributed table.
#
# The two fallback cases below pin serialize_query_plan = 0: with plan serialization the initiator
# plans the read of the underlying table itself and requires the caller's own SELECT on it, which
# these cases do not grant.
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY policy_05443 ON src_dist USING id != 1 TO $user"
echo -n "row policy on source "
"${client[@]}" --parallel_distributed_insert_select 2 --enable_analyzer 1 --enable_parallel_replicas 0 \
    --serialize_query_plan 0 \
    -q "INSERT INTO dst_dist SELECT * FROM src_dist" |& grep -o -m1 'ILLEGAL_PREWHERE' || echo 'NOT REJECTED'
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY policy_05443 ON src_dist"

# additional_table_filters is keyed by the source table name, which the optimized rewrite would
# drop, so it must fall back to the regular path, which applies the filter: the only row has
# id = 1, nothing is copied and the count stays the same.
echo -n "additional_table_filters "
"${client[@]}" --parallel_distributed_insert_select 2 --enable_parallel_replicas 0 --serialize_query_plan 0 \
    --additional_table_filters "{'src_dist' : 'id != 1'}" -q "INSERT INTO dst_dist SELECT * FROM src_dist"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH DISTRIBUTED dst_dist; SELECT count() FROM copy_local"

# A source from a `Remote` database whose shard points back to this server reads the underlying
# local table, so the caller's own SELECT on it is required (the stored credentials are only for
# genuinely remote servers), like the regular read path enforces.
remote_db="${CLICKHOUSE_DATABASE}_remote"
$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE $remote_db ENGINE = Remote('127.0.0.1', '$CLICKHOUSE_DATABASE', 'default', '');
    GRANT SHOW COLUMNS ON $CLICKHOUSE_DATABASE.* TO $user;
    GRANT SELECT, INSERT ON $remote_db.* TO $user;
    GRANT INSERT ON $CLICKHOUSE_DATABASE.copy_local TO $user;
"
echo -n "remote database proxy "
"${client[@]}" --parallel_distributed_insert_select 2 \
    -q "INSERT INTO $remote_db.copy_local SELECT * FROM $remote_db.secret_local" |&
    grep -o -m1 'ACCESS_DENIED' || echo 'NOT DENIED'

# ...and works once SELECT on the underlying table is granted.
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.secret_local TO $user"
"${client[@]}" --parallel_distributed_insert_select 2 \
    -q "INSERT INTO $remote_db.copy_local SELECT * FROM $remote_db.secret_local"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM copy_local"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE $remote_db;
    DROP USER $user;
"
