#!/usr/bin/env bash

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

echo "--- not supported"
$STREAMING_CLIENT -q "SELECT * FROM m STREAM CURSOR {'a': 1}" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"

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
$STREAMING_CLIENT --max_block_size=1 --min_insert_block_size_rows=1 --min_insert_block_size_bytes=0 -q "INSERT INTO ring SELECT number FROM numbers(1, 100)"
wait
grep -o -m1 "TOO_SLOW" "$fifo_3.out"
rm -f "$fifo_3.out"
cleanup "$fifo_3" "$pid_3" > /dev/null
