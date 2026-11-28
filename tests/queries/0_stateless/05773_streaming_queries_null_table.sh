#!/usr/bin/env bash

# A streaming query from a `Null` table receives every block inserted into the table while it runs:
# the table works as an ephemeral queue with many producers and many consumers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

$STREAMING_CLIENT -q "
    CREATE TABLE n (k String, v UInt64) ENGINE = Null;
    CREATE TABLE stored (k String, v UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE MATERIALIZED VIEW mv TO stored AS SELECT * FROM n WHERE k != 'probe';
"

echo "--- not supported"
$STREAMING_CLIENT -q "SELECT * FROM n STREAM CURSOR {'a': 1}" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
echo "--- bounded"
$STREAMING_CLIENT -q "SELECT count() FROM n STREAM BOUNDED"

# Wait until a subscription is active: the blocks inserted before it are not received.
wait_for_subscription()
{
    local fifo=$1
    (
        for _ in {1..600}; do
            [ -f "$fifo.ready" ] && break
            $CLICKHOUSE_CLIENT -q "INSERT INTO n VALUES ('probe', 0)"
            sleep 0.1
        done
    ) &
    local producer=$!
    read_until "$fifo" "probe" > /dev/null
    touch "$fifo.ready"
    wait $producer
    rm -f "$fifo.ready"
}

echo "--- two subscribers receive every block"
read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT k, v FROM n STREAM")
read -r fifo_2 pid_2 < <(spawn $STREAMING_CLIENT -q "SELECT k, v FROM n STREAM WHERE k IN ('b', 'probe')")
wait_for_subscription "$fifo_1"
wait_for_subscription "$fifo_2"

# Two producers.
$CLICKHOUSE_CLIENT -q "INSERT INTO n VALUES ('a', 1), ('b', 2)" &
$CLICKHOUSE_CLIENT -q "INSERT INTO n VALUES ('b', 3)" &
wait
$CLICKHOUSE_CLIENT -q "INSERT INTO n VALUES ('end', 4)"

echo "first"
read_until "$fifo_1" "end" | grep -v probe | sort
echo "second"
$CLICKHOUSE_CLIENT -q "INSERT INTO n VALUES ('b', 100)"
read_until "$fifo_2" "100" | grep -v probe | sort -n -k2
cleanup "$fifo_1" "$pid_1" > /dev/null
cleanup "$fifo_2" "$pid_2" > /dev/null

echo "--- the materialized view still gets the data"
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM stored ORDER BY ALL"

echo "--- a subscriber that falls behind fails instead of blocking the inserts"
$CLICKHOUSE_CLIENT -q "DROP TABLE mv"
read -r fifo_3 pid_3 < <(spawn $STREAMING_CLIENT -q "SELECT k, sleepEachRow(0.1) FROM n STREAM")
wait_for_subscription "$fifo_3"
# The output of a client that exits while nothing reads its FIFO is lost, so the reader starts first.
read_until "$fifo_3" "TOO_SLOW" > "$fifo_3.out" &
$CLICKHOUSE_CLIENT --max_block_size=1 --min_insert_block_size_rows=1 --min_insert_block_size_bytes=0 -q "INSERT INTO n SELECT 'x', number FROM numbers(3000)"
wait
grep -o -m1 "TOO_SLOW" "$fifo_3.out"
rm -f "$fifo_3.out"
cleanup "$fifo_3" "$pid_3" > /dev/null
