#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-parallel, no-fasttest: uses a server-wide failpoint that pauses the next lightweight update, whichever table it is on.
# no-replicated-database, no-shared-merge-tree: the failpoint is in the lightweight update of the plain `MergeTree`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A lightweight update reads only the parts up to the block number that it allocated. A part inserted after that
# is in the storage snapshot of a subquery of the update, but the row scan of the subquery does not read it.
# The trivial `count()` rewrite of `optimize_trivial_count_with_sparsity_filter` must not count its defaults either.

FAILPOINT="mt_lightweight_update_pause_after_block_allocation"
TABLE="t_sparsity_lwu_block_cutoff"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null || true
}
trap cleanup EXIT

for sparsity_filter in 0 1
do
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS $TABLE SYNC;

        CREATE TABLE $TABLE (id UInt64, x UInt64, y UInt64) ENGINE = MergeTree ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
            ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;

        SYSTEM STOP MERGES $TABLE;
        INSERT INTO $TABLE VALUES (1, 0, 0);

        SYSTEM ENABLE FAILPOINT $FAILPOINT;
    "

    # The update allocates its block number and pauses before its subquery is executed.
    $CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query "
        UPDATE $TABLE SET y = (SELECT count() FROM $TABLE WHERE x = 0) WHERE id = 1
        SETTINGS optimize_trivial_count_query = 1, optimize_trivial_count_with_sparsity_filter = $sparsity_filter,
            enable_analyzer = 1, enable_parallel_replicas = 0, make_distributed_plan = 0" &

    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

    # This part has a block number above the one of the update.
    $CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE VALUES (2, 0, 0)"

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
    wait

    $CLICKHOUSE_CLIENT --query "SELECT 'optimize_trivial_count_with_sparsity_filter = $sparsity_filter:', id, y FROM $TABLE ORDER BY id"
done
