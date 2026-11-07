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

# Collect the expired entries first, so that the cleanup thread usually removes them in one pass.
# `SYSTEM STOP CLEANUP` does not wait for an iteration that already runs, so the results do not depend on it.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP CLEANUP t_dedup_cleanup_batches"

# The cleanup only uses the creation order of the entries, so the expired entries can be created directly:
# 250 entries are more than two batches of 100 removals. They are older than the inserts below, so they are expired.
$CLICKHOUSE_CLIENT --async_insert=0 --query "
    INSERT INTO system.zookeeper (path, name, value)
    SELECT '$ZK_PATH/deduplication_hashes', 'expired_' || toString(number), '' FROM numbers(250)"

# A leftover entry with a child.
$CLICKHOUSE_CLIENT --async_insert=0 --query "INSERT INTO system.zookeeper (path, name, value) VALUES ('$ZK_PATH/deduplication_hashes', 'leftover', '')"
$CLICKHOUSE_CLIENT --async_insert=0 --query "INSERT INTO system.zookeeper (path, name, value) VALUES ('$ZK_PATH/deduplication_hashes/leftover', 'child', '')"

# One entry per block: 5 blocks fill the window.
INSERT_SETTINGS=(--insert_keeper_fault_injection_probability=0 --max_insert_threads=1 --max_block_size=1 --min_insert_block_size_rows=1 --min_insert_block_size_bytes=0)
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" --insert_deduplication_token=recent --query "INSERT INTO t_dedup_cleanup_batches SELECT number FROM numbers(5)"

COUNT_EXPIRED="SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH/deduplication_hashes' AND (name LIKE 'expired\_%' OR name = 'leftover')"
$CLICKHOUSE_CLIENT --query "SYSTEM START CLEANUP t_dedup_cleanup_batches"

for _ in {1..300}
do
    [ "$($CLICKHOUSE_CLIENT --query "$COUNT_EXPIRED")" -eq 0 ] && break
    sleep 0.2
done

$CLICKHOUSE_CLIENT --query "$COUNT_EXPIRED"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH/deduplication_hashes'"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_dedup_cleanup_batches"

# All 5 blocks are still in the window, so they are deduplicated. The insert adds no entries, so the cleanup
# thread that still runs cannot change the result.
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" --insert_deduplication_token=recent --query "INSERT INTO t_dedup_cleanup_batches SELECT number FROM numbers(5)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_dedup_cleanup_batches"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH/deduplication_hashes'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_dedup_cleanup_batches SYNC"
