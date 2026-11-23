#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest - requires SSL

# A shard reached over a cluster without a secret runs the query as the cluster user (`default`) while
# `initial_user` is the user of the initiator. Distributed index analysis over a cluster with a secret
# must not forward such an `initial_user`: it has to run as the user the shard reads as.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password;

    CREATE TABLE data (key Int, value Int) ENGINE = MergeTree ORDER BY key
    SETTINGS distributed_index_analysis_min_parts_to_activate = 0, distributed_index_analysis_min_indexes_bytes_to_activate = 0;
    SYSTEM STOP MERGES data;
    INSERT INTO data SELECT number, number FROM numbers(0, 100);
    INSERT INTO data SELECT number, number FROM numbers(100, 100);
    INSERT INTO data SELECT number, number FROM numbers(200, 100);

    CREATE TABLE dist AS data ENGINE = Distributed(test_shard_localhost, currentDatabase(), data);
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.data TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.dist TO $user;
"

# FIXME: does not work with distributed plan
$CLICKHOUSE_CLIENT --serialize_query_plan=0 --user "$user" -q "
    SELECT sum(key) FROM dist WHERE key > 10
    SETTINGS prefer_localhost_replica = 0, enable_parallel_replicas = 0, distributed_index_analysis = 1,
        max_parallel_replicas = 2,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas_interserver_secret';

    SELECT sum(key) FROM dist WHERE key GLOBAL NOT IN (SELECT arrayJoin(range(7)) FROM system.one)
    SETTINGS prefer_localhost_replica = 0, enable_parallel_replicas = 0, distributed_index_analysis = 1,
        max_parallel_replicas = 2,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas_interserver_secret';

    -- Not analyzed on the replicas: running as the cluster user, they would build the set without the row policies of the user.
    SELECT sum(key) FROM dist WHERE key NOT IN (SELECT arrayJoin(range(7)) FROM system.one)
    SETTINGS prefer_localhost_replica = 0, enable_parallel_replicas = 0, distributed_index_analysis = 1,
        max_parallel_replicas = 2,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas_interserver_secret';
"

$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;

    -- The secondary query on the shard runs as the cluster user and sends the analysis to the other replica.
    SELECT user, initial_user, normalizeQuery(query)
    FROM system.query_log
    -- NOLINT(current_database = currentDatabase())
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND endsWith(log_comment, '-$CLICKHOUSE_DATABASE')
        AND query_kind = 'Select'
        AND type = 'QueryFinish'
        AND NOT is_initial_query
    ORDER BY event_time_microseconds;

    DROP USER $user;
"
