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
        --query "$1"
}

cleanup()
{
    local status=$?
    local cleanup_status=0
    # Dropping the tables also disposes of their table-scoped action locks.
    query "DROP TABLE IF EXISTS point_fetch_fallback_replica SYNC" >/dev/null || cleanup_status=$?
    query "DROP TABLE IF EXISTS point_fetch_fallback_source SYNC" >/dev/null || cleanup_status=$?
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

zk_path="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/point_fetch_fallback"
source_parts_path="$zk_path/replicas/source/parts"

query "
    DROP TABLE IF EXISTS point_fetch_fallback_replica SYNC;
    DROP TABLE IF EXISTS point_fetch_fallback_source SYNC;

    CREATE TABLE point_fetch_fallback_source (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/point_fetch_fallback', 'source') ORDER BY n
    SETTINGS old_parts_lifetime = 0, cleanup_delay_period = 1, max_cleanup_delay_period = 1,
        cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0;

    CREATE TABLE point_fetch_fallback_replica (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/point_fetch_fallback', 'replica') ORDER BY n
    SETTINGS always_fetch_merged_part = 1;

    SYSTEM SYNC REPLICA point_fetch_fallback_replica;
    SYSTEM STOP MERGES point_fetch_fallback_source;
    SYSTEM STOP MERGES point_fetch_fallback_replica;
    SYSTEM STOP REPLICATION QUEUES point_fetch_fallback_replica;

    INSERT INTO point_fetch_fallback_source VALUES (1);
    INSERT INTO point_fetch_fallback_source VALUES (2);
    SYSTEM SYNC REPLICA point_fetch_fallback_replica PULL;
"

# Read the actual names instead of assuming particular block numbers.
original_parts=$(query "
    SELECT arraySort(groupArray(name)) FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_fallback_source' AND active")
original_queue_condition="
    SELECT count() = 2 AND countIf(type = 'GET_PART') = 2
        AND arraySort(groupArray(new_part_name)) = $original_parts
    FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'point_fetch_fallback_replica'"
wait_until 'the two original GET_PART entries' "$original_queue_condition"

query "SYSTEM STOP PULLING REPLICATION LOG point_fetch_fallback_replica"

# `STOP PULLING` does not wait for an in-flight pull. `SYNC ... PULL` takes the
# same pull mutex before checking the blocker, so its expected `ABORTED` is a
# barrier: no pull can still be running when we create the merge below.
if pulling_error=$(query "SYSTEM SYNC REPLICA point_fetch_fallback_replica PULL" 2>&1); then
    echo 'SYNC PULL unexpectedly succeeded while log pulling was stopped' >&2
    exit 1
fi
if [[ "$pulling_error" != *'(ABORTED)'* || "$pulling_error" != *'Log pulling is cancelled'* ]]; then
    echo "$pulling_error" >&2
    exit 1
fi

query "
    SYSTEM START MERGES point_fetch_fallback_source;
    OPTIMIZE TABLE point_fetch_fallback_source FINAL SETTINGS alter_sync = 1, optimize_throw_if_noop = 0;
    SYSTEM STOP MERGES point_fetch_fallback_source;
"

wait_until 'the source merge and deletion of its outdated originals' "
    SELECT count() = 1 AND countIf(active AND level > 0) = 1
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_fallback_source'"
covering_part=$(query "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_fallback_source' AND active")

# A local deletion alone is insufficient: the exact-part lookup reads Keeper.
# Do this inspection before the log marker so it cannot satisfy the fallback check.
wait_until 'only the covering part to remain in Keeper' "
    SELECT count() = 1 AND countIf(name = '$covering_part') = 1
    FROM system.zookeeper WHERE path = '$source_parts_path'"
query "
    SELECT 'source originals removed', count() = 1 AND countIf(active AND name = '$covering_part') = 1
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_fallback_source';

    SELECT 'covering part not queued', count() = 2 AND countIf(type = 'GET_PART') = 2
        AND arraySort(groupArray(new_part_name)) = $original_parts
        AND countIf(new_part_name = '$covering_part') = 0
    FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'point_fetch_fallback_replica';
"

# The table and queue together define `virtual_parts`: the replica is empty and
# its queue contains only the originals. The covering part exists only on source.
wait_until 'the follower to have no local parts' "
    SELECT count() = 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_fallback_replica'"
fetch_started_at=$(query 'SELECT now64(6)')
query "SYSTEM START REPLICATION QUEUES point_fetch_fallback_replica"

# `num_tries` is incremented before the lookup. Require a completed failed attempt
# as well, so an idle queue or an attempt not yet reaching Keeper cannot pass.
wait_until 'a failed GET_PART attempt with log pulling still stopped' "
    SELECT countIf(type = 'GET_PART' AND num_tries > 0
        AND NOT is_currently_executing AND last_exception LIKE '%NO_REPLICA_HAS_PART%') > 0
    FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'point_fetch_fallback_replica'"
query "SYSTEM STOP REPLICATION QUEUES point_fetch_fallback_replica"
wait_until 'all fetch attempts to finish' "
    SELECT countIf(is_currently_executing) = 0 FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'point_fetch_fallback_replica'"

# The missing-part checker can also list source parts after a failed fetch.
# Require an earlier failed point lookup on the same session and thread, so that
# checker traffic cannot hide a missing fallback in `findReplicaHavingCoveringPart`.
# The old strategy listed directly, so this assertion fails on an unpatched server.
query "
    SYSTEM FLUSH LOGS zookeeper_log;

    SELECT 'fallback lists source parts', count() > 0
    FROM system.zookeeper_log AS listing
    INNER JOIN
    (
        SELECT session_id, thread_id, min(xid) AS first_missing_part_xid
        FROM system.zookeeper_log
        WHERE event_date >= yesterday() AND event_time >= toDateTime64('$fetch_started_at', 6)
            AND type = 'Response' AND op_num = 'Exists' AND error = 'ZNONODE'
            AND path IN arrayMap(part -> concat('$source_parts_path/', part), $original_parts)
        GROUP BY session_id, thread_id
    ) AS missing USING (session_id, thread_id)
    WHERE listing.event_date >= yesterday() AND listing.event_time >= toDateTime64('$fetch_started_at', 6)
        AND listing.type = 'Response' AND listing.error = 'ZOK'
        AND listing.op_num IN ('List', 'SimpleList', 'FilteredList')
        AND listing.path = '$source_parts_path' AND has(listing.children, '$covering_part')
        AND listing.xid > missing.first_missing_part_xid;

    SELECT 'original GET_PART entries kept', count() = 2 AND countIf(type = 'GET_PART') = 2
        AND arraySort(groupArray(new_part_name)) = $original_parts
    FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'point_fetch_fallback_replica';

    SELECT 'unknown covering part not fetched', count() = 0 FROM point_fetch_fallback_replica;

    SYSTEM START PULLING REPLICATION LOG point_fetch_fallback_replica;
    SYSTEM SYNC REPLICA point_fetch_fallback_replica PULL;
    SYSTEM START MERGES point_fetch_fallback_replica;
    SYSTEM START REPLICATION QUEUES point_fetch_fallback_replica;
    SYSTEM SYNC REPLICA point_fetch_fallback_replica;

    SELECT 'replica data', arraySort(groupArray(n)) FROM point_fetch_fallback_replica;
    SELECT 'replica has covering part', count() = 1 AND countIf(name = '$covering_part') = 1
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_fallback_replica' AND active;
"
