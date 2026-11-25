#!/usr/bin/env bash
# Tags: long, replica, no-shared-merge-tree, no-replicated-database
# Tag no-replicated-database: the explicit `ReplicatedMergeTree` path gets `/auto_{shard}` appended on a replicated database, so the `zookeeper_log` path filter matches nothing
# no-shared-merge-tree: depends on the ZooKeeper layout of ReplicatedMergeTree dedup blocks

# DROP PART (and the cleanup thread dropping parts emptied by TTL) removes the deduplication
# blocks of the part's block range. For a merged part that covers many inserts, those removals
# must be sent in batches, not in the single multi-request that also creates the log entry:
# that request could exceed ZooKeeper's maximum message size (jute.maxbuffer, 1MB by default).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS drop_part_many_dedup"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE drop_part_many_dedup (k UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05875/drop_part_many_dedup', 'r1')
        ORDER BY k
        SETTINGS replicated_deduplication_window = 10000"

# 120 blocks -> 120 deduplication entries, then merge them into a single part.
$CLICKHOUSE_CLIENT --query "INSERT INTO drop_part_many_dedup SELECT number FROM numbers(120) ORDER BY ALL SETTINGS min_insert_block_size_rows=1, max_block_size=1"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE drop_part_many_dedup FINAL"

PART=$($CLICKHOUSE_CLIENT --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'drop_part_many_dedup' AND active")
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'drop_part_many_dedup' AND active"

$CLICKHOUSE_CLIENT --query "ALTER TABLE drop_part_many_dedup DROP PART '$PART'" --query_id "drop_part_many_dedup_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM drop_part_many_dedup"

# The deduplication entries are gone, so re-inserting the same blocks is not deduplicated.
$CLICKHOUSE_CLIENT --query "INSERT INTO drop_part_many_dedup SELECT number FROM numbers(120) ORDER BY ALL SETTINGS min_insert_block_size_rows=1, max_block_size=1"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM drop_part_many_dedup"

# Every sub-request of a multi shares its xid: group the Remove requests of this DROP PART by
# (session_id, xid). Pre-fix: one multi with 120 removals -> "0 0". Fixed: batches of <= 100 -> "1 1".
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
          AND query_id = 'drop_part_many_dedup_$CLICKHOUSE_DATABASE'
          AND match(path, '^/clickhouse/tables/' || currentDatabase() || '/test_05875/drop_part_many_dedup/(blocks|async_blocks|deduplication_hashes)/')
        GROUP BY session_id, xid
    )"

$CLICKHOUSE_CLIENT --query "DROP TABLE drop_part_many_dedup"
