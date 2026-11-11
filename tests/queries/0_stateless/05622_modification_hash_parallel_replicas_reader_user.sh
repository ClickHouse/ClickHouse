#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the test sets the parallel-replica settings itself.

# With parallel replicas, a replica of `cluster_for_parallel_replicas` that is read over TCP runs as the user
# configured for the cluster, even when its address is this server, and that user's profile can change the
# rows read. The `MergeTree` modification hash is computed under the caller's context, so it stays known only
# when every replica reads as the caller, or the whole read is the in-process local plan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="u_05622_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_05622;
    DROP USER IF EXISTS ${USER_NAME};
    CREATE TABLE t_05622 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_05622 VALUES (1);
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05622 TO ${USER_NAME};
"

SETTINGS="allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'read_tasks', automatic_parallel_replicas_mode = 0,
    parallel_replicas_local_plan = 1, max_execution_time_leaf = 0"

function check()
{
    local label=$1 user=$2 cluster=$3 prefer_local=$4
    ${CLICKHOUSE_CLIENT} --user "$user" -q "
        SELECT '$label', modification_hash IS NULL FROM system.tables
        WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 't_05622'
        SETTINGS ${SETTINGS}, cluster_for_parallel_replicas = '$cluster', parallel_replicas_prefer_local_replica = $prefer_local"
}

# The only replica of `test_shard_localhost` is this server, configured with the `default` user. The local plan
# reads it in-process; without the local plan it is read over TCP as `default`.
check 'local plan, another user' "$USER_NAME" test_shard_localhost 1
check 'over TCP, the configured user' default test_shard_localhost 0
check 'over TCP, another user' "$USER_NAME" test_shard_localhost 0
# A replica of this cluster is another server.
check 'remote replica' default test_cluster_one_shard_two_replicas 1

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE t_05622;
    DROP USER ${USER_NAME};
"
