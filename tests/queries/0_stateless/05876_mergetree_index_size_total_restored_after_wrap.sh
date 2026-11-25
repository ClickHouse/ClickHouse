#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-parallel: the test waits on a server-wide PAUSE failpoint that holds every plain MergeTree mutation; no-replicated-database, no-shared-merge-tree: the failpoint is in plain MergeTree.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_before_rename_part" 2>/dev/null || true' EXIT

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (a UInt64, b UInt64, INDEX idx a TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS columns_and_secondary_indices_sizes_lazy_calculation = 0,
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0"

# Each INSERT makes exactly one part, with identical bytes.
INSERT_ONE_PART="--max_block_size 65505 --min_insert_block_size_rows 1048449 --min_insert_block_size_bytes 268402944 --max_insert_block_size 1048449"
$CLICKHOUSE_CLIENT $INSERT_ONE_PART -q "INSERT INTO t SELECT number, number FROM numbers(1000)"

# The mutated part gets its index sizes at finalize, then stops before its commit.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_before_rename_part"
$CLICKHOUSE_CLIENT --mutations_sync 0 -q "ALTER TABLE t UPDATE b = b + 1 WHERE 1"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_mutate_task_pause_before_rename_part PAUSE"

# Committed without the index, the part's index size is not added; the drop mutation then subtracts it, wrapping the total.
$CLICKHOUSE_CLIENT --alter_sync 0 -q "ALTER TABLE t DROP INDEX idx"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_before_rename_part"
wait_for_all_mutations "t"

# Adding the same index size back onto the wrapped total.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t ADD INDEX idx a TYPE minmax GRANULARITY 1"
$CLICKHOUSE_CLIENT $INSERT_ONE_PART -q "INSERT INTO t SELECT number, number FROM numbers(1000)"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"
