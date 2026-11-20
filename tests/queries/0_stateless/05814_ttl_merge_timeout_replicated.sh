#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# no-replicated-database: two replicas with explicit names; no-shared-merge-tree: every replica executes the merge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `persist_ttl_merge_times`, replicas write the same `ttl.txt` and respect the TTL merges assigned by each other.

for r in r1 r2; do
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS t_ttl_merge_timeout_$r SYNC;
        CREATE TABLE t_ttl_merge_timeout_$r (key UInt64, d DateTime)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_ttl_merge_timeout', '$r')
        ORDER BY key
        TTL d
        SETTINGS persist_ttl_merge_times = 1, merge_with_ttl_timeout = 86400, max_replicated_merges_with_ttl_in_queue = 100,
            always_fetch_merged_part = 0, execute_merges_on_single_replica_time_threshold = 0;
    "
done

# Assign a merge on one replica while the other one does not select merges, then let the other one execute it too.
function insert_and_merge()
{
    local assigner=$1 other=$2 key=$3
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_ttl_merge_timeout_$other"
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_ttl_merge_timeout_$assigner VALUES ($key, now() - 1000), ($key + 1, '2100-01-01 00:00:00')"
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_ttl_merge_timeout_$assigner SETTINGS optimize_throw_if_noop = 0"

    for _ in {1..600}; do
        local pending
        pending=$($CLICKHOUSE_CLIENT -q "
            SELECT (SELECT count() FROM t_ttl_merge_timeout_$assigner WHERE d < now())
                + (SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_ttl_merge_timeout_$assigner')")
        [ "$pending" -eq 0 ] && break
        sleep 0.1
    done

    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_ttl_merge_timeout_$other"
    $CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_ttl_merge_timeout_$other"
}

# A merge with an error means that the merged part differs from the one committed by the other replica.
function merges()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS part_log;
        SELECT '$1', table, countIf(merge_reason = 'TTLDeleteMerge' AND error = 0), countIf(error != 0) FROM system.part_log
        WHERE database = currentDatabase() AND table LIKE 't_ttl_merge_timeout_r_' AND event_type = 'MergeParts'
        GROUP BY table ORDER BY table;
    "
}

insert_and_merge r1 r2 1
merges 'TTL merge assigned by r1'

# r2 has no due time in memory, but the part keeps the time of the TTL merge, so r2 assigns a regular merge.
insert_and_merge r2 r1 10
merges 'merge assigned by r2'

$CLICKHOUSE_CLIENT -q "SELECT key FROM t_ttl_merge_timeout_r1 ORDER BY key"
$CLICKHOUSE_CLIENT -q "SELECT key FROM t_ttl_merge_timeout_r2 ORDER BY key"

for r in r1 r2; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE t_ttl_merge_timeout_$r SYNC"
done
