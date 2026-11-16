#!/usr/bin/env bash
# Tags: shard, atomic-database

# STREAM from a `Distributed` table sends the query with STREAM to every shard.
# Its cursor is {shard number: <cursor of the table on the shard>}.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

$STREAMING_CLIENT -q "
    CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    CREATE TABLE d (a UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t);
    INSERT INTO t VALUES (1), (2);
    INSERT INTO t VALUES (3);
"

CURSOR="{'1': {'all': {'block_number': 1, 'block_offset': 1}}, '2': {'all': {'block_number': 1, 'block_offset': 0}}}"

for prefer_localhost_replica in 0 1; do
    echo "--- prefer_localhost_replica = $prefer_localhost_replica"
    opts=("--prefer_localhost_replica=$prefer_localhost_replica")

    echo "bounded, no cursor"
    $STREAMING_CLIENT "${opts[@]}" -q "SELECT _shard_num, a FROM d STREAM BOUNDED ORDER BY ALL"

    echo "bounded, with cursor"
    $STREAMING_CLIENT "${opts[@]}" -q "SELECT _shard_num, a FROM d STREAM BOUNDED CURSOR $CURSOR ORDER BY ALL"

    echo "remote table function, with cursor"
    $STREAMING_CLIENT "${opts[@]}" -q "SELECT _shard_num, a FROM remote('127.0.0.{1,2}', currentDatabase(), t) STREAM BOUNDED CURSOR $CURSOR ORDER BY ALL"
done

echo "--- unbounded, with cursor"
read -r fifo pid < <(spawn $STREAMING_CLIENT --prefer_localhost_replica=0 -q "SELECT _shard_num, a FROM d STREAM CURSOR {'1': {'all': {'block_number': 2, 'block_offset': 0}}, '2': {'all': {'block_number': 2, 'block_offset': 0}}}")
$STREAMING_CLIENT -q "INSERT INTO t VALUES (4)"
read_until "$fifo" "4" 2 | sort
cleanup "$fifo" "$pid" > /dev/null

echo "--- incremental refresh"
# Every shard reports its cursor: the remote ones in the `StreamingCursor` packet, the local ones directly.
$STREAMING_CLIENT -q "
    CREATE TABLE tgt_remote (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE tgt_local (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW mv_remote REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt_remote EMPTY AS SELECT a FROM d SETTINGS prefer_localhost_replica = 0;
    CREATE MATERIALIZED VIEW mv_local REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt_local EMPTY AS SELECT a FROM d SETTINGS prefer_localhost_replica = 1;
"
refresh()
{
    $STREAMING_CLIENT -q "
        SYSTEM REFRESH VIEW mv_remote; SYSTEM WAIT VIEW mv_remote;
        SYSTEM REFRESH VIEW mv_local; SYSTEM WAIT VIEW mv_local;
        SELECT '$1', 'remote', count(), sum(a) FROM tgt_remote;
        SELECT '$1', 'local', count(), sum(a) FROM tgt_local;
    "
}
refresh round1
$STREAMING_CLIENT -q "INSERT INTO t VALUES (5)"
refresh round2
refresh round3
$STREAMING_CLIENT -q "DROP TABLE mv_remote; DROP TABLE mv_local"
