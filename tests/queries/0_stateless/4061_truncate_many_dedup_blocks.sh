#!/usr/bin/env bash
# Tags: long, replica, no-shared-merge-tree
# no-shared-merge-tree: depends on the ZooKeeper layout of ReplicatedMergeTree dedup blocks

# Verify that TRUNCATE splits the ZooKeeper removal of deduplication blocks into
# several multi-requests instead of sending them all in a single one. An unbatched
# multi-request could exceed the maximum ZooKeeper message size (the default
# jute.maxbuffer of 1MB) and fail for tables with large deduplication windows.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS truncate_many_dedup1"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS truncate_many_dedup2"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE truncate_many_dedup1 (k UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04061/truncate_many_dedup', 'r1')
        ORDER BY k
        SETTINGS replicated_deduplication_window = 10000"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE truncate_many_dedup2 (k UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04061/truncate_many_dedup', 'r2')
        ORDER BY k
        SETTINGS replicated_deduplication_window = 10000"

$CLICKHOUSE_CLIENT --query "INSERT INTO truncate_many_dedup1 SELECT number FROM numbers(120) ORDER BY ALL SETTINGS min_insert_block_size_rows=1, max_block_size=1"

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA truncate_many_dedup2"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM truncate_many_dedup1"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM truncate_many_dedup2"

# This TRUNCATE removes all deduplication block entries from ZooKeeper. The
# removals must be split into batches of at most MULTI_BATCH_SIZE operations.
$CLICKHOUSE_CLIENT --query "TRUNCATE TABLE truncate_many_dedup1 SETTINGS replication_alter_partitions_sync = 2"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM truncate_many_dedup1"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM truncate_many_dedup2"

# Assert that the removals were actually batched. Every sub-request of a multi
# shares the multi's xid, so grouping the Remove sub-requests for this table's
# nodes by (session_id, xid) gives the number of removals per multi-request.
#   * The pre-fix code sends one multi-request with all removals, so the largest
#     group exceeds 100 and there is only a single group     -> prints "0 0".
#   * The fixed code splits them into batches of at most 100, so the largest
#     group is <= 100 and there is more than one group        -> prints "1 1".
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS zookeeper_log"

$CLICKHOUSE_CLIENT --query "
    SELECT
        max(removals_per_multi) <= 100 AS every_batch_within_limit,
        count() > 1 AS removals_were_batched
    FROM
    (
        SELECT count() AS removals_per_multi
        FROM system.zookeeper_log
        WHERE type = 'Request'
          AND op_num = 'Remove'
          AND match(path, '^/clickhouse/tables/' || currentDatabase() || '/test_04061/truncate_many_dedup/(blocks|async_blocks|deduplication_hashes)/')
        GROUP BY session_id, xid
    )"

$CLICKHOUSE_CLIENT --query "DROP TABLE truncate_many_dedup1"
$CLICKHOUSE_CLIENT --query "DROP TABLE truncate_many_dedup2"
