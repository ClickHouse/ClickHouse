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
    CREATE TABLE dst (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE dst_d (a UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dst, a);
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

    # Only the read of the distributed table takes the cursor of the shard; the other read of `t` keeps its own cursor.
    echo "join with another streaming read of the underlying table"
    $STREAMING_CLIENT "${opts[@]}" -q "
        SELECT l._shard_num, l.a, r.a
        FROM d AS l STREAM BOUNDED
        INNER JOIN t AS r STREAM BOUNDED CURSOR {'all': {'block_number': 1, 'block_offset': 1}}
        ON l.a = r.a
        ORDER BY ALL"

    # A serialized query plan does not keep the settings of STREAM, so the query text is sent to the shards.
    echo "serialize_query_plan = 1, with cursor"
    $STREAMING_CLIENT "${opts[@]}" --serialize_query_plan=1 -q "SELECT _shard_num, a FROM d STREAM BOUNDED CURSOR $CURSOR ORDER BY ALL"
    $STREAMING_CLIENT "${opts[@]}" --serialize_query_plan=1 -q "SELECT _shard_num, a FROM remote('127.0.0.{1,2}', currentDatabase(), t) STREAM BOUNDED CURSOR $CURSOR ORDER BY ALL"

    # `parallel_distributed_insert_select` would forward the whole cursor to every shard, so it is not used for STREAM.
    for parallel_distributed_insert_select in 0 1 2; do
        echo "INSERT SELECT, parallel_distributed_insert_select = $parallel_distributed_insert_select"
        $STREAMING_CLIENT "${opts[@]}" -q "
            TRUNCATE TABLE dst;
            INSERT INTO dst_d SELECT a FROM d STREAM BOUNDED CURSOR $CURSOR
            SETTINGS parallel_distributed_insert_select = $parallel_distributed_insert_select, distributed_foreground_insert = 1;
            SELECT groupArraySorted(10)(a) FROM dst;"
    done
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

echo "--- incremental refresh from a table without persisted block numbers on the shards"
# The cursor of such a table is not stable across merges, so every shard rejects it, as a direct source would be.
$STREAMING_CLIENT -q "
    CREATE TABLE t_plain (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0;
    CREATE TABLE d_plain (a UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_plain);
    CREATE TABLE tgt_plain (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_plain VALUES (1);
"
for prefer_localhost_replica in 0 1; do
    $STREAMING_CLIENT -q "CREATE MATERIALIZED VIEW mv_plain REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt_plain EMPTY AS SELECT a FROM d_plain SETTINGS prefer_localhost_replica = $prefer_localhost_replica"
    echo "prefer_localhost_replica = $prefer_localhost_replica: $($STREAMING_CLIENT -q "SYSTEM REFRESH VIEW mv_plain; SYSTEM WAIT VIEW mv_plain" 2>&1 | grep -o -m1 'must set enable_block_number_column = 1')"
    $STREAMING_CLIENT -q "DROP TABLE mv_plain"
done
$STREAMING_CLIENT -q "SELECT 'rows appended', count() FROM tgt_plain"
