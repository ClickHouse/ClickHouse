#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: creates a global SQL-managed cluster and two users.

# An async `Distributed` queue builds its `ConnectionPoolWithFailover` once. `priority` is copied
# there and is not part of `Address::operator==`. After the queue exists, `ALTER CLUSTER` that
# only swaps replica priorities must replace that pool. `internal_replication = 1` makes the shard
# one directory, so the next send chooses a replica by the pool's priority.
# Lower `priority` is preferred.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cluster="test_sql_cluster_05632"
data="data_05632"
dist="dist_05632"
user_a="prio_a_${CLICKHOUSE_DATABASE}"
user_b="prio_b_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${dist}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${data}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${cluster}" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_a}, ${user_b}" >/dev/null 2>&1 || :
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${dist}"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${data}"
$CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${cluster}"
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_a}, ${user_b}"

$CLICKHOUSE_CLIENT --query "CREATE USER ${user_a} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --query "CREATE USER ${user_b} IDENTIFIED WITH no_password"

$CLICKHOUSE_CLIENT --query "
CREATE CLUSTER ${cluster} (
    SHARD (
        internal_replication = 1,
        REPLICA (host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}, user = '${user_a}', priority = 1),
        REPLICA (host = '127.0.0.2', port = ${CLICKHOUSE_PORT_TCP}, user = '${user_b}', priority = 2)
    )
)
"

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${data} (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${dist} AS ${data} ENGINE = Distributed(${cluster}, currentDatabase(), ${data})"
$CLICKHOUSE_CLIENT --query "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.${data} TO ${user_a}, ${user_b}"

# The first send creates the queue and its pool from the priorities above.
$CLICKHOUSE_CLIENT --distributed_foreground_insert 0 --prefer_localhost_replica 0 --log_comment '05632_before' \
    --query "INSERT INTO ${dist} (x) SELECT 1"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH DISTRIBUTED ${dist}"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
SELECT if(user = '${user_a}', 'first_replica', user)
FROM system.query_log
WHERE event_date >= yesterday()
  AND type = 'QueryFinish'
  AND has(databases, currentDatabase())
  AND log_comment = '05632_before'
  AND user IN ('${user_a}', '${user_b}')
ORDER BY event_time_microseconds
LIMIT 1
"

$CLICKHOUSE_CLIENT --query "
ALTER CLUSTER ${cluster} (
    SHARD (
        internal_replication = 1,
        REPLICA (host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}, user = '${user_a}', priority = 2),
        REPLICA (host = '127.0.0.2', port = ${CLICKHOUSE_PORT_TCP}, user = '${user_b}', priority = 1)
    )
)
"

$CLICKHOUSE_CLIENT --distributed_foreground_insert 0 --prefer_localhost_replica 0 --log_comment '05632_after' \
    --query "INSERT INTO ${dist} (x) SELECT 1"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH DISTRIBUTED ${dist}"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
SELECT if(user = '${user_b}', 'second_replica', user)
FROM system.query_log
WHERE event_date >= yesterday()
  AND type = 'QueryFinish'
  AND has(databases, currentDatabase())
  AND log_comment = '05632_after'
  AND user IN ('${user_a}', '${user_b}')
ORDER BY event_time_microseconds
LIMIT 1
"
