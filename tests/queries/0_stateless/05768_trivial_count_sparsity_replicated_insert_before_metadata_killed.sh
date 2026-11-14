#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, zookeeper, no-shared-merge-tree
# no-parallel: uses a server-wide failpoint that pauses every lookup of column defaultness stats.
# no-fasttest: tests that enable a fail point run alone, and they are kept out of the fast test.
# no-shared-merge-tree: the test exercises the mutation bookkeeping of `ReplicatedMergeTreeQueue`, which `SharedMergeTree` does not use.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ReplicatedMergeTree`: a part is inserted while `DROP COLUMN x, ADD COLUMN x Int64` is registered but the new
# metadata is not installed on the replica yet. The part has a data version higher than the block number of the
# mutation, but the old metadata version, so the `DROP COLUMN x` is applied to its reads on the fly by the metadata
# version. The query captures the part, then the mutation is removed with `KILL MUTATION` before the defaultness
# stats are consumed. The stats must be checked against the mutations of the query's own snapshot, the same as
# the row scan of the query uses, so `num_defaults` of the dropped `x` must not be used.

FAILPOINT="mt_column_defaultness_stats_pause_after_parts_snapshot"
TABLE="t_sparsity_replicated_insert_before_metadata"
OUTPUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_${TABLE}.out"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "SYSTEM START REPLICATION QUEUES $TABLE" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null || true
    rm -f "$OUTPUT"
}
trap cleanup EXIT

# The seed part makes the mutation get a block number in the partition, and dropping it leaves only the part
# inserted later.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $TABLE SYNC;

    CREATE TABLE $TABLE (id Int64, x Int64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/$TABLE', 'r1') ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;

    SYSTEM STOP MERGES $TABLE;
    INSERT INTO $TABLE VALUES (0, 0);
    ALTER TABLE $TABLE DROP PART 'all_0_0_0';

    SYSTEM STOP REPLICATION QUEUES $TABLE;
    ALTER TABLE $TABLE DROP COLUMN x, ADD COLUMN x Int64 SETTINGS alter_sync = 0, mutations_sync = 0;
    INSERT INTO $TABLE VALUES (1, 1), (2, 2);
    SYSTEM START REPLICATION QUEUES $TABLE;
"

# The replica applies the new metadata asynchronously. Wait until the reads apply the pending
# `DROP COLUMN x, ADD COLUMN x Int64` to the part inserted with the old metadata.
for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM $TABLE WHERE x = 0 SETTINGS optimize_trivial_count_with_sparsity_filter = 0") == 2 ]] && break
    sleep 0.1
done

PART=$($CLICKHOUSE_CLIENT --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active")
# The part was inserted with the old metadata, so only its metadata version makes the mutation pending for it.
$CLICKHOUSE_CLIENT --query "
    SELECT 'the part has a data version higher than the mutation:',
        (SELECT data_version FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active)
        > (SELECT block_numbers.number[1] FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE')"

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

$CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE database = currentDatabase() AND table = '$TABLE' SYNC FORMAT Null"
$CLICKHOUSE_CLIENT --query "
    SELECT 'the mutation history is empty:', count() = 0
    FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
wait

echo "count with the rewrite and the row scan:"
cat "$OUTPUT"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0 FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 1 HOUR AND logger_name LIKE '%$TABLE%'
        AND message = 'No defaultness stats for column x: part $PART has a pending metadata mutation of this column'"
