#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, zookeeper, no-shared-merge-tree
# no-parallel: uses a server-wide failpoint that pauses every lookup of column defaultness stats,
# and reconnects the server to ZooKeeper.
# no-fasttest: tests that enable a fail point run alone, and they are kept out of the fast test.
# no-shared-merge-tree: the test exercises the mutation bookkeeping of `ReplicatedMergeTreeQueue`, which `SharedMergeTree` does not use.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ReplicatedMergeTree`: the query captures the part while `DROP COLUMN x, ADD COLUMN x Int64` is pending,
# then the replication queue is reinitialized after a reconnect to ZooKeeper, which clears the local mutation
# history before it is loaded again. Pulling the replication log is stopped, so the history stays empty
# while the query consumes the defaultness stats. The captured part is still active and stores the dropped `x`
# without defaults under the queried name, while the row scan of the same query still applies the mutation,
# so `num_defaults` must not be used for it.

FAILPOINT="mt_column_defaultness_stats_pause_after_parts_snapshot"
TABLE="t_sparsity_replicated_mutations_cleared_by_reconnect"
OUTPUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_${TABLE}.out"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "SYSTEM START PULLING REPLICATION LOG $TABLE" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null || true
    rm -f "$OUTPUT"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $TABLE SYNC;

    CREATE TABLE $TABLE (id Int64, x Int64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/$TABLE', 'r1') ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;

    SYSTEM STOP MERGES $TABLE;
    INSERT INTO $TABLE VALUES (1, 1), (2, 2);

    ALTER TABLE $TABLE DROP COLUMN x, ADD COLUMN x Int64 SETTINGS alter_sync = 0, mutations_sync = 0;
"

PART=$($CLICKHOUSE_CLIENT --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active")

QUERY="SELECT count(),
        (SELECT count() FROM $TABLE WHERE x = 0 SETTINGS optimize_trivial_count_with_sparsity_filter = 0)
    FROM $TABLE WHERE x = 0
    SETTINGS optimize_trivial_count_query = 1, optimize_trivial_count_with_sparsity_filter = 1,
        enable_analyzer = 1, enable_parallel_replicas = 0, make_distributed_plan = 0,
        use_query_condition_cache = 0, use_statistics_for_part_pruning = 0"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --query "$QUERY" > "$OUTPUT" &

# The settings above make the query reach the lookup of the defaultness stats, the timeout only turns
# a regression of that into a failure instead of a hang.
timeout 120 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP PULLING REPLICATION LOG $TABLE"
$CLICKHOUSE_CLIENT --query "SYSTEM RECONNECT ZOOKEEPER"

# The queue is reinitialized asynchronously after the reconnect.
for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'") == 0 ]] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT --query "
    SELECT 'the mutation history is empty:', count() = 0
    FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
wait

# The part is still active, but the commands it needs are not known at the moment, so the rewrite is declined
# and the count matches the row scan.
echo "count with the rewrite and the row scan:"
cat "$OUTPUT"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0 FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 1 HOUR AND logger_name LIKE '%$TABLE%'
        AND message LIKE 'No defaultness stats for column x: part $PART has data version %, and a mutation with version % that may be required for it was removed from the history'"
