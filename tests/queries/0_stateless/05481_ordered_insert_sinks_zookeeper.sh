#!/usr/bin/env bash
# Tags: zookeeper

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

INSERT_SETTINGS=(
    --max_threads=4 --max_insert_threads=4
    --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0
    --use_concurrency_control_for_max_threads=0 --use_strict_insert_block_limits=0 --deduplicate_insert=never
    --async_insert=0
)

$CLICKHOUSE_CLIENT --multiquery -q "
    CREATE TABLE order_rep_rmt (k UInt32, v UInt64)
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/order_rep_rmt', '1') ORDER BY k;
    CREATE TABLE order_rep_cmt (k UInt32, sign Int8, v UInt64)
        ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/{database}/order_rep_cmt', '1', sign) ORDER BY k;
    CREATE TABLE order_rep_coalescing (k UInt32, v Nullable(UInt64))
        ENGINE = ReplicatedCoalescingMergeTree('/clickhouse/tables/{database}/order_rep_coalescing', '1') ORDER BY k;
    CREATE TABLE order_rep_versioned (k UInt32, v UInt64)
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/order_rep_versioned', '1', v) ORDER BY k;
    CREATE TABLE order_keeper (k UInt32, v UInt64)
        ENGINE = KeeperMap('/' || currentDatabase() || '/order_keeper') PRIMARY KEY k;
"

for table in order_rep_rmt order_rep_coalescing order_keeper order_rep_versioned; do
    echo "$table"
    $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
        "EXPLAIN PIPELINE INSERT INTO $table VALUES (0, 0)" | grep -cE "ReplicatedMergeTreeSink|KeeperMapSink"
done
echo order_rep_cmt
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
    "EXPLAIN PIPELINE INSERT INTO order_rep_cmt VALUES (0, 1, 0)" | grep -c "ReplicatedMergeTreeSink"

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE order_rep_rmt SYNC;
    DROP TABLE order_rep_cmt SYNC;
    DROP TABLE order_rep_coalescing SYNC;
    DROP TABLE order_rep_versioned SYNC;
    DROP TABLE order_keeper SYNC;
"
