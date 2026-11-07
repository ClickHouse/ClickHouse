#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# no-replicated-database: uses two explicit replicas on the same server.
# no-shared-merge-tree: checks replica-specific part discovery in Keeper.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

query()
{
    $CLICKHOUSE_CLIENT --multiquery --format TSV --receive_timeout 60 \
        --async_insert 0 --insert_keeper_fault_injection_probability 0 --use_query_cache 0 \
        --apply_mutations_on_fly 0 \
        --query "$1"
}

cleanup()
{
    local status=$?
    local cleanup_status=0
    query "DROP TABLE IF EXISTS point_fetch_intermediate_replica SYNC" >/dev/null || cleanup_status=$?
    query "DROP TABLE IF EXISTS point_fetch_intermediate_source SYNC" >/dev/null || cleanup_status=$?
    if [[ "$status" != 0 ]]; then
        return "$status"
    fi
    return "$cleanup_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_until()
{
    local description=$1
    local condition=$2
    local result
    for _ in {1..600}; do
        clickhouse_test_time_left || return 1
        result=$(query "$condition") || return 1
        if [[ "$result" == 1 ]]; then
            return 0
        fi
        sleep 0.1
    done
    echo "Timed out waiting for $description (last result: $result)" >&2
    return 1
}

zk_path="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/point_fetch_intermediate"
source_parts_path="$zk_path/replicas/source/parts"

query "
    DROP TABLE IF EXISTS point_fetch_intermediate_replica SYNC;
    DROP TABLE IF EXISTS point_fetch_intermediate_source SYNC;

    CREATE TABLE point_fetch_intermediate_source (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/point_fetch_intermediate', 'source') ORDER BY tuple()
    SETTINGS old_parts_lifetime = 3600;

    CREATE TABLE point_fetch_intermediate_replica (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/point_fetch_intermediate', 'replica') ORDER BY tuple();

    SYSTEM SYNC REPLICA point_fetch_intermediate_replica;
    SYSTEM STOP MERGES point_fetch_intermediate_source;
    SYSTEM STOP MERGES point_fetch_intermediate_replica;
    SYSTEM STOP REPLICATION QUEUES point_fetch_intermediate_replica;

    INSERT INTO point_fetch_intermediate_source VALUES (1);
    INSERT INTO point_fetch_intermediate_source VALUES (2);
"
original_parts=$(query "
    SELECT arraySort(groupArray(name)) FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_intermediate_source' AND active")

query "
    SYSTEM START MERGES point_fetch_intermediate_source;
    OPTIMIZE TABLE point_fetch_intermediate_source FINAL SETTINGS alter_sync = 1, optimize_throw_if_noop = 0, optimize_skip_merged_partitions = 1;
"
wait_until 'the intermediate merged part' "
    SELECT countIf(active) = 1 AND countIf(active AND level > 0) = 1
        AND countIf(NOT active AND has($original_parts, name)) = 2
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_intermediate_source'"
merged_part=$(query "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_intermediate_source' AND active")

# Publish a mutation result that nobody can produce yet. The source only fetches
# mutations, and the empty follower keeps its merges and mutations stopped.
query "
    ALTER TABLE point_fetch_intermediate_source MODIFY SETTING always_fetch_mutated_part = 1;
    ALTER TABLE point_fetch_intermediate_source UPDATE n = n + 10 WHERE 1 SETTINGS mutations_sync = 0;
"
wait_until 'the future mutation entry' "
    SELECT count() = 1 FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'point_fetch_intermediate_source'
        AND type = 'MUTATE_PART' AND parts_to_merge = ['$merged_part']"
future_part=$(query "
    SELECT new_part_name FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'point_fetch_intermediate_source'
        AND type = 'MUTATE_PART' AND parts_to_merge = ['$merged_part']")
query "SYSTEM SYNC REPLICA point_fetch_intermediate_replica PULL"
wait_until 'the follower to know originals, merge and future mutation' "
    SELECT countIf(type = 'GET_PART' AND has($original_parts, new_part_name)) = 2
        AND countIf(type = 'MERGE_PARTS' AND new_part_name = '$merged_part') = 1
        AND countIf(type = 'MUTATE_PART' AND new_part_name = '$future_part') = 1
    FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'point_fetch_intermediate_replica'"

# Check Keeper before the marker: retained original znodes make an original-exact
# fallback choose the small parts instead of the available intermediate merge.
query "
    SELECT 'original parts retained', count() = 3
        AND countIf(active AND name = '$merged_part') = 1
        AND countIf(NOT active AND has($original_parts, name)) = 2
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_intermediate_source';

    SELECT 'original znodes retained', arraySort(groupArray(name)) = arraySort(arrayPushBack($original_parts, '$merged_part'))
    FROM system.zookeeper WHERE path = '$source_parts_path';

    SELECT 'future mutation unavailable', count() = 0 FROM system.parts
    WHERE database = currentDatabase()
        AND table IN ('point_fetch_intermediate_source', 'point_fetch_intermediate_replica')
        AND name = '$future_part';
"
fetch_started_at=$(query 'SELECT now64(6)')
query "
    SYSTEM START REPLICATION QUEUES point_fetch_intermediate_replica;
    SYSTEM SYNC REPLICA point_fetch_intermediate_replica LIGHTWEIGHT;

    SELECT 'replica before mutation', arraySort(groupArray(n)) FROM point_fetch_intermediate_replica;
    SELECT 'intermediate part fetched', count() = 1 AND countIf(name = '$merged_part') = 1
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_intermediate_replica' AND active;

    SELECT 'future mutation still queued', count() = 1 FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'point_fetch_intermediate_replica'
        AND type = 'MUTATE_PART' AND new_part_name = '$future_part';

    SYSTEM FLUSH LOGS zookeeper_log;
    SELECT 'future miss lists intermediate', count() > 0
    FROM system.zookeeper_log AS listing
    INNER JOIN
    (
        SELECT session_id, thread_id, min(xid) AS first_missing_part_xid
        FROM system.zookeeper_log
        WHERE event_date >= yesterday() AND event_time >= toDateTime64('$fetch_started_at', 6)
            AND type = 'Response' AND op_num = 'Exists' AND error = 'ZNONODE'
            AND path = '$source_parts_path/$future_part'
        GROUP BY session_id, thread_id
    ) AS missing USING (session_id, thread_id)
    WHERE listing.event_date >= yesterday() AND listing.event_time >= toDateTime64('$fetch_started_at', 6)
        AND listing.type = 'Response' AND listing.error = 'ZOK'
        AND listing.op_num IN ('List', 'SimpleList', 'FilteredList')
        AND listing.path = '$source_parts_path' AND has(listing.children, '$merged_part')
        AND listing.xid > missing.first_missing_part_xid;

    ALTER TABLE point_fetch_intermediate_source MODIFY SETTING always_fetch_mutated_part = 0;
    SYSTEM SYNC REPLICA point_fetch_intermediate_source;
    SYSTEM START MERGES point_fetch_intermediate_replica;
    SYSTEM SYNC REPLICA point_fetch_intermediate_replica;
"
wait_until 'both mutations to be finalized' "
    SELECT count() = 2 AND countIf(is_done) = 2 FROM system.mutations
    WHERE database = currentDatabase()
        AND table IN ('point_fetch_intermediate_source', 'point_fetch_intermediate_replica')"

query "
    SELECT 'replica after mutation', arraySort(groupArray(n)) FROM point_fetch_intermediate_replica;
    SELECT 'mutated part active', count() = 1 AND countIf(name = '$future_part') = 1
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_intermediate_replica' AND active;
    SELECT 'mutations complete', count() = 2 AND countIf(is_done) = 2 FROM system.mutations
    WHERE database = currentDatabase()
        AND table IN ('point_fetch_intermediate_source', 'point_fetch_intermediate_replica');
"
