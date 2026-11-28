#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: uses a server-wide failpoint that pauses every lookup of column defaultness stats.
# no-fasttest: tests that enable a fail point run alone, and they are kept out of the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `optimize_trivial_count_with_sparsity_filter` answers `count() WHERE x = 0` from `num_defaults` in
# `serialization.json` of the base parts. Here a lightweight `UPDATE` is materialized by `OPTIMIZE ... FINAL`
# and its patch part is removed after the query took its storage snapshot, but before the defaultness stats
# are consumed. The table has no patch parts by then, while the reads of the captured base part still apply
# the captured patch, so the rewrite must be rejected.

FAILPOINT="mt_column_defaultness_stats_pause_after_parts_snapshot"
TABLE="t_sparsity_patch_materialized_after_parts_snapshot"
OUTPUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_${TABLE}.out"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null || true
    rm -f "$OUTPUT"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $TABLE SYNC;

    CREATE TABLE $TABLE (x Int64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1,
        enable_block_number_column = 1, enable_block_offset_column = 1,
        merge_tree_clear_old_parts_interval_seconds = 1, cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0;

    SYSTEM STOP MERGES $TABLE;
    INSERT INTO $TABLE VALUES (1), (2);

    UPDATE $TABLE SET x = 0 WHERE 1;

    SYSTEM ENABLE FAILPOINT $FAILPOINT;
"

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM $TABLE WHERE x = 0
    SETTINGS optimize_trivial_count_query = 1, optimize_trivial_count_with_sparsity_filter = 1, apply_patch_parts = 1,
        enable_analyzer = 1, enable_parallel_replicas = 0, make_distributed_plan = 0, empty_result_for_aggregation_by_empty_set = 0
" > "$OUTPUT" &

# The settings above make the query reach the lookup of the defaultness stats, the timeout only turns
# a regression of that into a failure instead of a hang.
timeout 120 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

# Materialize the patch and wait until it is removed while the query holds the captured snapshot.
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $TABLE"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL"
for _ in $(seq 1 600)
do
    patches=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active AND name LIKE 'patch-%'")
    [[ "$patches" == "0" ]] && break
    sleep 0.1
done
echo "active patch parts: $patches"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
wait

echo "count with the rewrite: $(cat "$OUTPUT")"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM $TABLE WHERE x = 0 SETTINGS optimize_trivial_count_with_sparsity_filter = 0"
