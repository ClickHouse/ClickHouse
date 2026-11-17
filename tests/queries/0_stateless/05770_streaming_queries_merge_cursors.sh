#!/usr/bin/env bash
# Tags: atomic-database

# STREAM from a `Merge` table reads every table with STREAM. Its cursor is {database: {table: <cursor of the table>}}.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

DB=$CLICKHOUSE_DATABASE

$STREAMING_CLIENT -q "
    CREATE TABLE t1 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    CREATE TABLE t2 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    CREATE TABLE m (a UInt64) ENGINE = Merge(currentDatabase(), '^t[12]\$');
    INSERT INTO t1 VALUES (1), (2);
    INSERT INTO t1 VALUES (3);
    INSERT INTO t2 VALUES (10);
    INSERT INTO t2 VALUES (20), (30);
"

echo "--- bounded, no cursor"
$STREAMING_CLIENT -q "SELECT _table, a, _block_number, _block_offset FROM m STREAM BOUNDED ORDER BY ALL"

CURSOR="{'$DB': {'t1': {'all': {'block_number': 1, 'block_offset': 1}}, 't2': {'all': {'block_number': 2, 'block_offset': 0}}}}"

echo "--- bounded, with cursor"
$STREAMING_CLIENT -q "SELECT _table, a FROM m STREAM BOUNDED CURSOR $CURSOR ORDER BY ALL"

echo "--- merge table function, with cursor"
$STREAMING_CLIENT -q "SELECT _table, a FROM merge('^t[12]\$') STREAM BOUNDED CURSOR $CURSOR ORDER BY ALL"

echo "--- a table without STREAM support"
$STREAMING_CLIENT -q "CREATE TABLE t3 (a UInt64) ENGINE = Log"
$STREAMING_CLIENT -q "SELECT a FROM merge('^t[13]\$') STREAM BOUNDED" 2>&1 | grep -o -m1 "ILLEGAL_STREAM"

echo "--- unbounded, with cursor"
read -r fifo pid < <(spawn $STREAMING_CLIENT -q "SELECT _table, a FROM m STREAM CURSOR {'$DB': {'t1': {'all': {'block_number': 2, 'block_offset': 0}}, 't2': {'all': {'block_number': 2, 'block_offset': 1}}}}")
$STREAMING_CLIENT -q "INSERT INTO t1 VALUES (4)"
read_until "$fifo" "4"
$STREAMING_CLIENT -q "INSERT INTO t2 VALUES (40)"
read_until "$fifo" "40"
cleanup "$fifo" "$pid" > /dev/null

echo "--- incremental refresh"
$STREAMING_CLIENT -q "
    CREATE TABLE tgt (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY AS SELECT a FROM m;
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
    SELECT 'round1', count(), sum(a) FROM tgt;
    INSERT INTO t1 VALUES (5);
    INSERT INTO t2 VALUES (50);
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
    SELECT 'round2', count(), sum(a) FROM tgt;
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
    SELECT 'round3', count(), sum(a) FROM tgt;
    DROP TABLE mv;
"
