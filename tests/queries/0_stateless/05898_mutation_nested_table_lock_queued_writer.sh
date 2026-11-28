#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-shared-merge-tree, no-fasttest
# no-parallel: waits on a global pauseable failpoint, which a predicate mutation of a concurrent test could hit.
# no-replicated-database: `ON CLUSTER` is not allowed there.
# no-shared-merge-tree: the failpoint is in the `ReplicatedMergeTree` block allocation path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mutation statement holds a share lock on its table and takes it again in the partition pruning
# analysis. An exclusive lock requested in between must not make the second acquisition wait for the
# first one: the statement would wait for itself until `lock_acquire_timeout`.

trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutation_prune_pause_before_analysis"' EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t (p UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', '1')
    PARTITION BY p ORDER BY tuple()
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    CREATE TABLE t_alias ENGINE = Alias('t');

    INSERT INTO t VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0);
"

function run_case()
{
    local name=$1
    local query=$2
    shift 2
    local writer_query_id="${CLICKHOUSE_DATABASE}_writer_${name}"
    local output="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}.out"

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT rmt_mutation_prune_pause_before_analysis"

    $CLICKHOUSE_CLIENT --lock_acquire_timeout 5 --optimize_mutations_with_partition_pruning 1 --mutations_sync 0 \
        --enable_lightweight_update 1 --distributed_ddl_output_mode none "$@" \
        --query "$query" > "$output" 2>&1 &
    local statement_pid=$!

    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT rmt_mutation_prune_pause_before_analysis PAUSE"

    # Atomic POPULATE takes the source table exclusively, so it queues behind the paused statement.
    $CLICKHOUSE_CLIENT --lock_acquire_timeout 300 --materialized_views_populate_atomically 1 --query_id "$writer_query_id" \
        --query "CREATE MATERIALIZED VIEW mv_${name} ENGINE = Memory POPULATE AS SELECT p, v FROM t" &
    local writer_pid=$!

    local writer_queued=0
    for _ in {1..300}; do
        if [[ $($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.processes WHERE query_id = '$writer_query_id'
                AND (SELECT value FROM system.metrics WHERE metric = 'RWLockWaitingWriters') >= 1") == 1 ]]; then
            writer_queued=1
            break
        fi
        sleep 0.1
    done
    [[ $writer_queued == 1 ]] || echo "$name writer not queued"

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutation_prune_pause_before_analysis"
    wait $statement_pid $writer_pid

    if grep -q DEADLOCK_AVOIDED "$output"; then
        echo "$name DEADLOCK_AVOIDED"
    elif [[ -s "$output" ]]; then
        echo "$name $(cat "$output")"
    else
        echo "$name ok"
    fi
    rm -f "$output"
}

run_case c1 "UPDATE ${CLICKHOUSE_DATABASE}.t ON CLUSTER test_shard_localhost SET v = 1 WHERE p = 1"
run_case c2 "ALTER TABLE ${CLICKHOUSE_DATABASE}.t ON CLUSTER test_shard_localhost UPDATE v = 2 WHERE p = 2" --alter_update_mode heavy
run_case c3 "ALTER TABLE t DELETE WHERE p = 3"
run_case c4 "ALTER TABLE ${CLICKHOUSE_DATABASE}.t ON CLUSTER test_shard_localhost UPDATE v = 4 WHERE p = 4" --alter_update_mode lightweight_force
run_case c5 "UPDATE ${CLICKHOUSE_DATABASE}.t_alias ON CLUSTER test_shard_localhost SET v = 5 WHERE p = 5"
run_case c6 "DELETE FROM ${CLICKHOUSE_DATABASE}.t ON CLUSTER test_shard_localhost WHERE p = 6" --lightweight_delete_mode lightweight_update_force

for _ in {1..600}; do
    [[ $($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't' AND NOT is_done") == 0 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
    SELECT p, v FROM t ORDER BY p;
    SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE 'mv\_%';

    DROP TABLE mv_c1;
    DROP TABLE mv_c2;
    DROP TABLE mv_c3;
    DROP TABLE mv_c4;
    DROP TABLE mv_c5;
    DROP TABLE mv_c6;
    DROP TABLE t_alias;
    DROP TABLE t SYNC;
"
