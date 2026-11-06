#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Every local shard of a foreground Distributed INSERT opens its own sink on the same URL table concurrently.
# A fresh URL table per iteration, because only its first writes can race (under TSan).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE dist (x UInt64) ENGINE = Distributed(test_cluster_five_shards_localhost, currentDatabase(), url_sink, rand());"

for _ in $(seq 1 7); do
    echo "DROP TABLE IF EXISTS url_sink;
CREATE TABLE url_sink (x UInt64) ENGINE = URL('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=INSERT+INTO+${CLICKHOUSE_DATABASE}.dst+FORMAT+TSV', TSV);
INSERT INTO dist SELECT number FROM numbers(1000) SETTINGS distributed_foreground_insert = 1, prefer_localhost_replica = 1;"
done | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT -q "SELECT count() FROM dst"
