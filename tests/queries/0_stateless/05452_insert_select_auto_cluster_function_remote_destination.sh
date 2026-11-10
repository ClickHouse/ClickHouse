#!/usr/bin/env bash

# https://github.com/ClickHouse/ClickHouse/issues/121347
#
# `INSERT INTO FUNCTION remote(...) SELECT * FROM url(...)` with parallel replicas must insert the rows exactly
# once, not once per destination shard. `parallel_replicas_for_cluster_engines` converts the plain `url` into
# a cluster storage on the initiator, so the query takes the distributed `INSERT ... SELECT` path, which
# forwarded the plain function to every shard, and each shard read all the files. Forwarding it as
# `urlCluster` needs a cluster name the shards know, and a `remote(...)` destination has none, so the
# distributed execution is skipped for the auto-converted `url`. An explicit `urlCluster` carries its own
# cluster name and is still forwarded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Three single-row URLs, so a shard that reads all of them instead of its share changes the count.
URL="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT%20{1,2,3}"
# A deterministic sharding key would skip the distributed execution before the cluster name is checked, hence `rand()`.
DST="remote('${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP},${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 'dst_05452', rand())"

# `enable_parallel_replicas`, `max_parallel_replicas` and `cluster_for_parallel_replicas` allow converting
# `url` into a cluster storage. `parallel_distributed_insert_select = 2` takes the distributed path, as the
# default does, but the test runner can randomize it. `distributed_foreground_insert = 1` makes the rows
# reach `dst_05452` before the `INSERT` returns.
$CLICKHOUSE_CLIENT -q "
    SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        parallel_distributed_insert_select = 2, distributed_foreground_insert = 1;

    DROP TABLE IF EXISTS dst_05452;
    CREATE TABLE dst_05452 (x UInt32) ENGINE = MergeTree ORDER BY x;

    INSERT INTO FUNCTION ${DST} SELECT * FROM url('${URL}', 'TSV', 'x UInt32')
    SETTINGS parallel_replicas_for_cluster_engines = 0;
    SELECT 'url', count(), uniqExact(x) FROM dst_05452;
    TRUNCATE TABLE dst_05452;

    INSERT INTO FUNCTION ${DST} SELECT * FROM url('${URL}', 'TSV', 'x UInt32')
    SETTINGS parallel_replicas_for_cluster_engines = 1;
    SELECT 'url, auto-converted', count(), uniqExact(x) FROM dst_05452;
    TRUNCATE TABLE dst_05452;

    INSERT INTO FUNCTION ${DST} SELECT * FROM urlCluster('test_cluster_one_shard_three_replicas_localhost', '${URL}', 'TSV', 'x UInt32');
    SELECT 'urlCluster', count(), uniqExact(x) FROM dst_05452;

    DROP TABLE dst_05452;
"
