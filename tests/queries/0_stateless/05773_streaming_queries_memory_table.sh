#!/usr/bin/env bash
# Tags: atomic-database

# A streaming query from a `Memory` table returns the blocks that the table has, and then every block inserted later.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

$STREAMING_CLIENT -q "
    CREATE TABLE m (k String, v UInt64) ENGINE = Memory;
    INSERT INTO m VALUES ('a', 1);
    INSERT INTO m VALUES ('b', 2);
"

echo "--- a cursor that is not of a Memory table"
$STREAMING_CLIENT -q "SELECT * FROM m STREAM CURSOR {'a': 1}" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

echo "--- bounded"
$STREAMING_CLIENT -q "SELECT k, v FROM m STREAM BOUNDED"

echo "--- unbounded"
read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT k, v FROM m STREAM")
read_until "$fifo_1" "2"
$STREAMING_CLIENT -q "INSERT INTO m VALUES ('c', 3)"
read_until "$fifo_1" "3"

read -r fifo_2 pid_2 < <(spawn $STREAMING_CLIENT -q "SELECT v FROM m STREAM WHERE k != 'b'")
read_until "$fifo_2" "3"

# Two producers.
$STREAMING_CLIENT -q "INSERT INTO m VALUES ('d', 4)" &
$STREAMING_CLIENT -q "INSERT INTO m VALUES ('e', 5)" &
wait
$STREAMING_CLIENT -q "INSERT INTO m VALUES ('f', 6)"
echo "first"
read_until "$fifo_1" "6" | sort
echo "second"
read_until "$fifo_2" "6" | sort

echo "--- the data is replaced by a mutation"
# The output of a client that exits while nothing reads its FIFO is lost, so the readers start first.
read_until "$fifo_1" "ABORTED" > "$fifo_1.out" &
read_until "$fifo_2" "ABORTED" > "$fifo_2.out" &
$STREAMING_CLIENT -q "ALTER TABLE m UPDATE v = v + 1 WHERE 1"
wait
grep -o -m1 "ABORTED" "$fifo_1.out"
grep -o -m1 "ABORTED" "$fifo_2.out"
rm -f "$fifo_1.out" "$fifo_2.out"
cleanup "$fifo_1" "$pid_1" > /dev/null
cleanup "$fifo_2" "$pid_2" > /dev/null

echo "--- a stream that falls behind max_rows_to_keep"
$STREAMING_CLIENT -q "CREATE TABLE ring (v UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 10"
read -r fifo_3 pid_3 < <(spawn $STREAMING_CLIENT --max_block_size=1 -q "SELECT v, sleepEachRow(0.5) FROM ring STREAM")
$STREAMING_CLIENT -q "INSERT INTO ring VALUES (0)"
read_until "$fifo_3" "0" > /dev/null
# The output of a client that exits while nothing reads its FIFO is lost, so the reader starts first.
read_until "$fifo_3" "TOO_SLOW" > "$fifo_3.out" &
# Separate inserts: the blocks of one insert are added at once (with a single insert thread), and the table does not remove
# the blocks it has just got, so the stream would not fall behind.
$STREAMING_CLIENT -q "$(for i in $(seq 1 100); do echo "INSERT INTO ring VALUES ($i);"; done)"
wait
grep -o -m1 "TOO_SLOW" "$fifo_3.out"
rm -f "$fifo_3.out"
cleanup "$fifo_3" "$pid_3" > /dev/null

echo "--- incremental refresh"
refresh()
{
    $STREAMING_CLIENT -q "SYSTEM REFRESH VIEW mv; SYSTEM WAIT VIEW mv; SELECT '$1', count(), sum(v) FROM tgt"
}
$STREAMING_CLIENT -q "
    CREATE TABLE src (v UInt64) ENGINE = Memory;
    INSERT INTO src VALUES (1), (2);
    CREATE TABLE tgt (v UInt64) ENGINE = MergeTree ORDER BY v;
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY AS SELECT v FROM src;
"
refresh round1
$STREAMING_CLIENT -q "INSERT INTO src VALUES (3)"
refresh round2
refresh round3
# After TRUNCATE, all the blocks are new.
$STREAMING_CLIENT -q "TRUNCATE TABLE src; INSERT INTO src VALUES (10)"
refresh round4
# A mutation rewrites the rows that were already appended.
$STREAMING_CLIENT -q "ALTER TABLE src UPDATE v = v + 1 WHERE 1"
$STREAMING_CLIENT -q "SYSTEM REFRESH VIEW mv; SYSTEM WAIT VIEW mv" 2>&1 | grep -o -m1 "was rewritten by a mutation"
$STREAMING_CLIENT -q "DROP TABLE mv"
