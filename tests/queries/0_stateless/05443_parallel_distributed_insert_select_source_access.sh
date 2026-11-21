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
# the regular path. The regular path reads `src_dist` with a `SELECT` sent to the shard, while the
# optimized rewrite sends only an `INSERT ... SELECT` there, so the fallback is detected by the
# kind of the secondary queries in `system.query_log`. (The regular path of this branch does not
# enforce a row policy on a Distributed table, so the result of the copy itself is not checked.)
#
# The two fallback cases below pin serialize_query_plan = 0: with plan serialization the initiator
# plans the read of the underlying table itself and requires the caller's own SELECT on it, which
# these cases do not grant.
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY policy_05443 ON src_dist USING id != 1 TO $user"
query_id="05443_row_policy_${CLICKHOUSE_DATABASE}_$RANDOM"
"${client[@]}" --parallel_distributed_insert_select 2 --enable_analyzer 1 --enable_parallel_replicas 0 \
    --serialize_query_plan 0 --query_id "$query_id" \
    -q "INSERT INTO dst_dist SELECT * FROM src_dist"
$CLICKHOUSE_CLIENT -q "
    DROP ROW POLICY policy_05443 ON src_dist;
    SYSTEM FLUSH DISTRIBUTED dst_dist;
    TRUNCATE TABLE copy_local;
    SYSTEM FLUSH LOGS query_log;
    SELECT
        'row policy on source',
        if(countIf(query_kind = 'Select') > 0 AND countIf(query_kind = 'Insert' AND query ILIKE '%SELECT%') = 0,
            'FALLBACK', 'OPTIMIZED')
    FROM system.query_log
    WHERE event_date >= yesterday() AND initial_query_id = '$query_id' AND NOT is_initial_query AND type = 'QueryFinish'
"

# additional_table_filters is keyed by the source table name, which the optimized rewrite would
# drop, so it must fall back to the regular path, which applies the filter: the only row has
# id = 1, nothing is copied and the destination stays empty.
echo -n "additional_table_filters "
"${client[@]}" --parallel_distributed_insert_select 2 --enable_parallel_replicas 0 --serialize_query_plan 0 \
    --additional_table_filters "{'src_dist' : 'id != 1'}" -q "INSERT INTO dst_dist SELECT * FROM src_dist"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH DISTRIBUTED dst_dist; SELECT count() FROM copy_local"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
