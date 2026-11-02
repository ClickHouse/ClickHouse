#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-fasttest, no-shared-merge-tree
# no-replicated-database: the explicit ZooKeeper path is used as is
# no-shared-merge-tree: relies on the ZooKeeper layout of ReplicatedMergeTree

# The cleanup thread removes the expired deduplication entries in batches. Check that
#  - more expired entries than fit in one batch are all removed,
#  - an expired entry with children (a leftover of previous server versions) is removed too,
#  - the entries inside the window are kept, so the inserts that are still in the window are deduplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_TEST_ZOOKEEPER_PREFIX="${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}"
ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_dedup_cleanup_batches"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dedup_cleanup_batches SYNC"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_dedup_cleanup_batches (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_dedup_cleanup_batches', '{replica}')
    ORDER BY n
    SETTINGS replicated_deduplication_window = 5, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0"

# Collect the expired entries first: the cleanup thread removes them all in one pass.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP CLEANUP t_dedup_cleanup_batches"

# The leftover is the oldest entry, so it is expired.
$CLICKHOUSE_CLIENT --async_insert=0 --query "INSERT INTO system.zookeeper (path, name, value) VALUES ('$ZK_PATH/deduplication_hashes', 'leftover', '')"
$CLICKHOUSE_CLIENT --async_insert=0 --query "INSERT INTO system.zookeeper (path, name, value) VALUES ('$ZK_PATH/deduplication_hashes/leftover', 'child', '')"

# One entry per block: 250 blocks are more than two batches of 100 removals.
INSERT_SETTINGS=(--insert_keeper_fault_injection_probability=0 --max_insert_threads=1 --max_block_size=1 --min_insert_block_size_rows=1 --min_insert_block_size_bytes=0)
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" --insert_deduplication_token=bulk --query "INSERT INTO t_dedup_cleanup_batches SELECT number FROM numbers(250)"
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" --insert_deduplication_token=recent --query "INSERT INTO t_dedup_cleanup_batches SELECT 1000"

COUNT_ENTRIES="SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH/deduplication_hashes'"
$CLICKHOUSE_CLIENT --query "$COUNT_ENTRIES" # 250 + 1 + leftover

$CLICKHOUSE_CLIENT --query "SYSTEM START CLEANUP t_dedup_cleanup_batches"

for _ in {1..300}
do
    [ "$($CLICKHOUSE_CLIENT --query "$COUNT_ENTRIES")" -le 5 ] && break
    sleep 0.2
done

$CLICKHOUSE_CLIENT --query "$COUNT_ENTRIES"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH/deduplication_hashes' AND name = 'leftover'"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_dedup_cleanup_batches"

# The last insert is still in the window, so it is deduplicated.
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" --insert_deduplication_token=recent --query "INSERT INTO t_dedup_cleanup_batches SELECT 1000"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_dedup_cleanup_batches"

# The window holds `recent` and the last 4 blocks of `bulk`: only these 4 blocks are deduplicated.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP CLEANUP t_dedup_cleanup_batches"
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" --insert_deduplication_token=bulk --query "INSERT INTO t_dedup_cleanup_batches SELECT number FROM numbers(250)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_dedup_cleanup_batches"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_dedup_cleanup_batches SYNC"
