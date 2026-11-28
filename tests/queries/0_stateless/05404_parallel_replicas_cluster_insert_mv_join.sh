#!/usr/bin/env bash

# An INSERT SELECT forwarded to `*Cluster` workers must not start a coordinated read in a materialized view
# that joins a MergeTree table: nothing coordinates it there.
# https://github.com/ClickHouse/ClickHouse/issues/126127

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE dim (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO dim SELECT number, toString(number) FROM numbers(20);
    CREATE TABLE local_ins (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE dist_ins AS local_ins ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), local_ins);
    CREATE TABLE mv_target (x UInt64, v String) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW mv_ins TO mv_target AS SELECT l.x AS x, dim.v AS v FROM local_ins AS l INNER JOIN dim ON l.x = dim.k;
"

for mode in 0 1; do
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE local_ins; TRUNCATE TABLE mv_target;"
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO dist_ins SELECT x FROM url('http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(10)', TSV, 'x UInt64')
        SETTINGS async_insert = 0, parallel_distributed_insert_select = 2, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = $mode,
            max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_two_shards_localhost',
            parallel_replicas_for_cluster_engines = 1, parallel_replicas_for_non_replicated_merge_tree = 1"
    $CLICKHOUSE_CLIENT -q "SELECT 'mode', $mode, (SELECT count() FROM local_ins), (SELECT (count(), sum(x)) FROM mv_target)"
done
