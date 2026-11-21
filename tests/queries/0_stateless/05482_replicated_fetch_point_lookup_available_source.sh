#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# no-replicated-database: uses three explicit replicas on the same server.
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
    query "DROP TABLE IF EXISTS point_fetch_available_t SYNC" >/dev/null || cleanup_status=$?
    query "DROP TABLE IF EXISTS point_fetch_available_r SYNC" >/dev/null || cleanup_status=$?
    query "DROP TABLE IF EXISTS point_fetch_available_s SYNC" >/dev/null || cleanup_status=$?
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

zk_path="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/point_fetch_available"

query "
    DROP TABLE IF EXISTS point_fetch_available_t SYNC;
    DROP TABLE IF EXISTS point_fetch_available_r SYNC;
    DROP TABLE IF EXISTS point_fetch_available_s SYNC;

    CREATE TABLE point_fetch_available_s (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/point_fetch_available', 's') ORDER BY n
    SETTINGS old_parts_lifetime = 0, cleanup_delay_period = 1, max_cleanup_delay_period = 1,
        cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0;

    CREATE TABLE point_fetch_available_r (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/point_fetch_available', 'r') ORDER BY n;

    CREATE TABLE point_fetch_available_t (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/point_fetch_available', 't') ORDER BY n;

    SYSTEM SYNC REPLICA point_fetch_available_r;
    SYSTEM SYNC REPLICA point_fetch_available_t;
    SYSTEM STOP MERGES point_fetch_available_s;
    SYSTEM STOP MERGES point_fetch_available_r;
    SYSTEM STOP MERGES point_fetch_available_t;
    SYSTEM STOP REPLICATION QUEUES point_fetch_available_t;

    INSERT INTO point_fetch_available_s VALUES (1);
    INSERT INTO point_fetch_available_s VALUES (2);
    SYSTEM SYNC REPLICA point_fetch_available_r;
"

original_parts=$(query "
    SELECT arraySort(groupArray(name)) FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_available_r' AND active")
wait_until 'R to have both originals' "
    SELECT count() = 2 AND arraySort(groupArray(n)) = [1, 2] FROM point_fetch_available_r"
query "SYSTEM STOP PULLING REPLICATION LOG point_fetch_available_r"

# `SYNC ... PULL` takes the pull mutex before checking the blocker. Its expected
# `ABORTED` is a barrier against an in-flight pull learning the merge on R.
if pulling_error=$(query "SYSTEM SYNC REPLICA point_fetch_available_r PULL" 2>&1); then
    echo 'SYNC PULL unexpectedly succeeded while log pulling was stopped' >&2
    exit 1
fi
if [[ "$pulling_error" != *'(ABORTED)'* || "$pulling_error" != *'Log pulling is cancelled'* ]]; then
    echo "$pulling_error" >&2
    exit 1
fi

query "
    SYSTEM START MERGES point_fetch_available_s;
    OPTIMIZE TABLE point_fetch_available_s FINAL
        SETTINGS alter_sync = 1, optimize_throw_if_noop = 0, optimize_skip_merged_partitions = 1;
    SYSTEM STOP MERGES point_fetch_available_s;
    SYSTEM SYNC REPLICA point_fetch_available_t PULL;
"
wait_until 'S to retain only the merge' "
    SELECT count() = 1 AND countIf(active AND level > 0) = 1 FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_available_s'"
merged_part=$(query "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_available_s' AND active")
wait_until 'S to publish only the merge in Keeper' "
    SELECT count() = 1 AND countIf(name = '$merged_part') = 1
    FROM system.zookeeper WHERE path = '$zk_path/replicas/s/parts'"
wait_until 'R to publish only the originals in Keeper' "
    SELECT count() = 2 AND arraySort(groupArray(name)) = $original_parts
    FROM system.zookeeper WHERE path = '$zk_path/replicas/r/parts'"
wait_until 'T to know the merge and both original GET_PART entries' "
    SELECT countIf(type = 'GET_PART') = 2
        AND arraySort(groupArrayIf(new_part_name, type = 'GET_PART')) = $original_parts
        AND countIf(type = 'MERGE_PARTS' AND new_part_name = '$merged_part') = 1
    FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'point_fetch_available_t'"
wait_until 'T to have no local parts' "
    SELECT count() = 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_available_t'"

# S stays active and advertises the known merge, but cannot send it. A global
# preference for that merge would exclude R, which can still send the originals.
query "
    SYSTEM STOP REPLICATED SENDS point_fetch_available_s;
    SELECT 'source stays active', countIf(name = 'is_active') = 1
    FROM system.zookeeper WHERE path = '$zk_path/replicas/s';
    SYSTEM START REPLICATION QUEUES point_fetch_available_t;
"
wait_until 'T to fetch both rows with no remaining GET_PART while S cannot send' "
    SELECT (SELECT arraySort(groupArray(n)) FROM point_fetch_available_t) = [1, 2]
        AND (SELECT count() FROM system.replication_queue
            WHERE database = currentDatabase() AND table = 'point_fetch_available_t'
                AND type = 'GET_PART') = 0"

# Keep S blocked through all assertions. With merges stopped, the active part
# names prove T fetched P/Q rather than the covering M. Only R can send P/Q.
query "
    SELECT 'target before source recovery', arraySort(groupArray(n)) FROM point_fetch_available_t;
    SELECT 'remaining GET_PART', count() FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'point_fetch_available_t' AND type = 'GET_PART';
    SELECT 'original parts fetched', count() = 2 AND arraySort(groupArray(name)) = $original_parts
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'point_fetch_available_t' AND active;

    SYSTEM START REPLICATED SENDS point_fetch_available_s;
"
