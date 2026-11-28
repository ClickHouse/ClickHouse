#!/usr/bin/env bash
# Tags: atomic-database

# A streaming query from a `Memory` table reads the columns like an ordinary query: it applies the row policies and `PREWHERE`,
# decompresses the columns of a table with `compress = 1`, and fills the columns absent from the blocks inserted before `ALTER`.
# An unbounded stream finishes when the downstream does not need more rows, without waiting for another insert.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

user="user_${CLICKHOUSE_DATABASE}"
policy="policy_${CLICKHOUSE_DATABASE}"

$STREAMING_CLIENT -q "
    CREATE TABLE m (k UInt64, v String) ENGINE = Memory;
    INSERT INTO m VALUES (1, 'allowed'), (2, 'forbidden');
    DROP USER IF EXISTS $user;
    CREATE USER $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.m TO $user;
    CREATE ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.m USING k = 1 TO $user;
"

echo "--- row policy, bounded"
$STREAMING_CLIENT --user "$user" -q "SELECT k, v FROM ${CLICKHOUSE_DATABASE}.m STREAM BOUNDED ORDER BY k"
$STREAMING_CLIENT --user "$user" -q "SELECT k, v FROM ${CLICKHOUSE_DATABASE}.m STREAM BOUNDED PREWHERE v != '' ORDER BY k"

echo "--- row policy, unbounded"
read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT --user "$user" -q "SELECT k, v FROM ${CLICKHOUSE_DATABASE}.m STREAM")
read_until "$fifo_1" "allowed" > /dev/null
$STREAMING_CLIENT -q "INSERT INTO m VALUES (2, 'forbidden again'), (1, 'allowed again')"
read_until "$fifo_1" "allowed again"
cleanup "$fifo_1" "$pid_1" > /dev/null

$STREAMING_CLIENT -q "DROP ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.m; DROP USER $user"

echo "--- PREWHERE"
$STREAMING_CLIENT -q "SELECT k, v FROM m STREAM BOUNDED PREWHERE k = 2 ORDER BY v"
$STREAMING_CLIENT -q "SELECT v FROM m STREAM BOUNDED PREWHERE k = 2 WHERE v LIKE '%again' ORDER BY v"

echo "--- compressed table"
$STREAMING_CLIENT -q "
    CREATE TABLE c (k UInt64, v String, a Array(UInt64)) ENGINE = Memory SETTINGS compress = 1;
    INSERT INTO c VALUES (1, 'one', [1]);
    INSERT INTO c SELECT number, toString(number), range(number % 3) FROM numbers(2, 1000);
    SELECT k, v, a, a.size0 FROM c STREAM BOUNDED ORDER BY k LIMIT 3;
    SELECT count(), sum(k), sum(length(v)), sum(a.size0) FROM c STREAM BOUNDED;
"

echo "--- a column added after the blocks were inserted"
$STREAMING_CLIENT -q "
    CREATE TABLE a (x UInt64) ENGINE = Memory;
    INSERT INTO a VALUES (1);
    ALTER TABLE a ADD COLUMN y UInt64 DEFAULT 7, ADD COLUMN s String;
    INSERT INTO a VALUES (2, 3, 'new');
    SELECT x, y, s FROM a STREAM BOUNDED ORDER BY x;
"

echo "--- an unbounded stream finishes when it has enough rows"
# There are two blocks, so the stream waits for a third one after `LIMIT 1` has got its row.
$STREAMING_CLIENT --max_block_size=1 -q "SELECT x FROM a STREAM LIMIT 1"
$STREAMING_CLIENT -q "SELECT x, sleepEachRow(0.1) FROM a STREAM LIMIT 2 FORMAT Null"
echo "done"
