#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree

# The cursor of a `ReplicatedMergeTree` table refers to the block numbers allocated in Keeper,
# so a stream can be resumed on another replica with the cursor obtained on one replica.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

# A Keeper fault injected after a block number is allocated makes the insert retry with a new block number,
# which would make the block numbers, and so the cursors below, nondeterministic.
STREAMING_CLIENT="$STREAMING_CLIENT --insert_keeper_fault_injection_probability=0"

$STREAMING_CLIENT -q "
    CREATE TABLE r1 (p UInt8, a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/r', 'r1') ORDER BY a PARTITION BY p SETTINGS $STREAMING_TABLE_SETTINGS;
    CREATE TABLE r2 (p UInt8, a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/r', 'r2') ORDER BY a PARTITION BY p SETTINGS $STREAMING_TABLE_SETTINGS;
    INSERT INTO r1 VALUES (0, 1), (0, 2), (1, 10);
    INSERT INTO r2 VALUES (0, 3), (1, 20);
    SYSTEM SYNC REPLICA r1;
    SYSTEM SYNC REPLICA r2;
"

echo "--- positions on r1"
$STREAMING_CLIENT -q "SELECT _partition_id, a, _block_number, _block_offset FROM r1 STREAM BOUNDED ORDER BY ALL"

# The block numbers of a replicated table start at 0.
CURSOR="{'0': {'block_number': 0, 'block_offset': 0}, '1': {'block_number': 0, 'block_offset': 0}}"

echo "--- resume on r1"
$STREAMING_CLIENT -q "SELECT a FROM r1 STREAM BOUNDED CURSOR $CURSOR ORDER BY ALL"
echo "--- resume on r2"
$STREAMING_CLIENT -q "SELECT a FROM r2 STREAM BOUNDED CURSOR $CURSOR ORDER BY ALL"

echo "--- unbounded on r2, inserts on r1"
read -r fifo pid < <(spawn $STREAMING_CLIENT -q "SELECT a FROM r2 STREAM CURSOR {'0': {'block_number': 1, 'block_offset': 0}, '1': {'block_number': 1, 'block_offset': 0}}")
$STREAMING_CLIENT -q "INSERT INTO r1 VALUES (0, 4)"
read_until "$fifo" "4"
$STREAMING_CLIENT -q "INSERT INTO r1 VALUES (1, 30)"
read_until "$fifo" "30"
cleanup "$fifo" "$pid" > /dev/null

$STREAMING_CLIENT -q "DROP TABLE r1 SYNC; DROP TABLE r2 SYNC"
