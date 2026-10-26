#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: creates and drops a global SQL-managed cluster

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

# A replica of a SQL-managed cluster pointing to this server must be detected as local,
# both with an explicit port and with the port inherited from the server's `tcp_port`.

cluster="c_sql_cluster_local_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP CLUSTER IF EXISTS ${cluster};
    CREATE CLUSTER ${cluster} (
        SHARD (host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}),
        SHARD (host = '127.0.0.1')
    );
    SELECT shard_num, is_local, port = ${CLICKHOUSE_PORT_TCP} FROM system.clusters WHERE cluster = '${cluster}' ORDER BY shard_num;
"

# An insert into a `Distributed` table goes directly into the local shards instead of the background queue.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_local (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_dist AS t_local ENGINE = Distributed(${cluster}, currentDatabase(), t_local, x);
    INSERT INTO t_dist SETTINGS distributed_foreground_insert = 0 SELECT number FROM numbers(10);
    SELECT count() FROM t_local;
    SELECT count() FROM t_dist SETTINGS prefer_localhost_replica = 1;
    DROP TABLE t_dist;
    DROP TABLE t_local;
    DROP CLUSTER ${cluster};
"
