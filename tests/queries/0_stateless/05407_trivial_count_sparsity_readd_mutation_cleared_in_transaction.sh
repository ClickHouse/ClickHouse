#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-ordinary-database, no-replicated-database
# no-parallel: uses a server-wide failpoint that pauses every lookup of column defaultness stats.
# no-fasttest: tests that enable a fail point run alone, and they are kept out of the fast test.
# no-ordinary-database: the test uses transactions.
# no-replicated-database: MergeTree becomes ReplicatedMergeTree there, and a transaction that writes is rejected for it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

# The same interleaving as `05406_trivial_count_sparsity_readd_mutation_cleared_after_parts_snapshot`, but the
# reader runs in a transaction, and another uncommitted transaction drops the captured part. The part becomes
# `Outdated` but stays visible to the reader, while the `DROP COLUMN x, ADD COLUMN x Int64` is marked as done
# (no active part needs it) and its record is removed from the mutation history. The captured part still stores
# the dropped `x` without defaults under the queried name, so `num_defaults` must not be used for it.

FAILPOINT="mt_column_defaultness_stats_pause_after_parts_snapshot"
TABLE="t_sparsity_readd_mutation_cleared_in_transaction"
OUTPUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_${TABLE}.out"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    wait || true
    tx 1 "ROLLBACK" >/dev/null 2>&1 || true
    tx 2 "ROLLBACK" >/dev/null 2>&1 || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null || true
    rm -f "$OUTPUT"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $TABLE SYNC;

    CREATE TABLE $TABLE (id Int64, x Int64, z Int64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1,
        finished_mutations_to_keep = 1, merge_tree_clear_old_parts_interval_seconds = 1,
        cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0;

    SYSTEM STOP MERGES $TABLE;
    INSERT INTO $TABLE VALUES (1, 1, 1), (2, 2, 2);

    ALTER TABLE $TABLE DROP COLUMN x, ADD COLUMN x Int64 SETTINGS alter_sync = 0, mutations_sync = 0;
    ALTER TABLE $TABLE DROP COLUMN z SETTINGS alter_sync = 0, mutations_sync = 0;
"

PART=$($CLICKHOUSE_CLIENT --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active")

QUERY="SELECT count() FROM $TABLE WHERE x = 0
    SETTINGS optimize_trivial_count_query = 1, optimize_trivial_count_with_sparsity_filter = 1,
        enable_analyzer = 1, enable_parallel_replicas = 0, make_distributed_plan = 0, empty_result_for_aggregation_by_empty_set = 0"

tx 1 "BEGIN TRANSACTION"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
tx 1 "$QUERY" > "$OUTPUT" &

# The settings above make the query reach the lookup of the defaultness stats, the timeout only turns
# a regression of that into a failure instead of a hang.
timeout 120 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

tx 2 "BEGIN TRANSACTION"
tx 2 "ALTER TABLE $TABLE DROP PART '$PART'"

# The mutations are done once no active part needs them, and the record of the
# `DROP COLUMN x, ADD COLUMN x Int64` is removed from the history.
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $TABLE"
for _ in $(seq 1 600)
do
    only_drop_z_left=$($CLICKHOUSE_CLIENT --query "
        SELECT count() = 1 AND min(is_done) AND any(command) LIKE '%DROP COLUMN z%' FROM system.mutations
        WHERE database = currentDatabase() AND table = '$TABLE'")
    [[ "$only_drop_z_left" == "1" ]] && break
    sleep 0.1
done
echo "only the drop of z is left in the mutation history: $only_drop_z_left"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
wait

# The part is still visible to the reader, but the commands it needs are no longer known, so the rewrite is declined.
echo "count with the rewrite:"
cat "$OUTPUT"

tx 1 "ROLLBACK"
tx 2 "ROLLBACK"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0 FROM system.text_log
    WHERE event_date >= yesterday() AND logger_name LIKE '%$TABLE%'
        AND message LIKE 'No defaultness stats for column x: part $PART has data version %, and a mutation with version % that may be required for it was removed from the history'"
