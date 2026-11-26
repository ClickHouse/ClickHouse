#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-parallel: the failpoints are server-global and one-shot, so a concurrent test could consume or clear the pause the holder waits in.
# no-fasttest: needs Keeper and failpoints.
# no-replicated-database: an UPDATE of a non-replicated table goes through the DDL queue there, so the holder would not run on this client.
# no-shared-merge-tree: the plain part pauses at a failpoint of the plain MergeTree.

# In update_parallel_mode = 'auto' a lightweight UPDATE also writes the MATERIALIZED columns derived from the columns it
# sets, computed from the stored values of their other inputs. So it conflicts with an update that writes one of those
# inputs or reads one of those columns, while an update of the same input stays concurrent.
# A holder updates `a` and `t` and is paused while it holds the lock; each waiter runs with lock_acquire_timeout = 0, so
# it is refused at once on a conflict. The refused ones are re-run after the holder, then every row must have c = a + b.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP_PLAIN=mt_lightweight_update_pause_after_block_allocation
FP_PIPELINE=completed_pipeline_pause_before_teardown
# Only a query whose id starts with this prefix can park at FP_PIPELINE.
QID_PREFIX=completed_pipeline_pause_failpoint_

SETTINGS="SET enable_lightweight_update = 1; SET update_parallel_mode = 'auto'; SET insert_keeper_fault_injection_probability = 0;"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_PLAIN" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_PIPELINE" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_lwu_mat_plain SYNC; DROP TABLE IF EXISTS t_lwu_mat_rmt SYNC" 2>/dev/null || true
}
trap cleanup EXIT

function arm()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $1"
    if [[ "$($CLICKHOUSE_CLIENT --query "SELECT enabled FROM system.fail_points WHERE name = '$1'")" != 1 ]]
    then
        echo "Failed to arm $1" >&2
        exit 2
    fi
}

function wait_parked()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $1 PAUSE"
    # The failpoint is one-shot, so it is disarmed once the holder has parked there.
    if [[ "$($CLICKHOUSE_CLIENT --query "SELECT enabled FROM system.fail_points WHERE name = '$1'")" != 0 ]]
    then
        echo "The holder did not park at $1" >&2
        exit 2
    fi
}

function waiter()
{
    local label=$1
    local query=$2
    local err
    local refused

    err=$($CLICKHOUSE_CLIENT --query "$SETTINGS SET lock_acquire_timeout = 0; $query" 2>&1 >/dev/null)
    if [[ -z "$err" ]]
    then
        refused=0
    elif [[ "$err" == *TIMEOUT_EXCEEDED* ]]
    then
        refused=1
        refused_queries+=("$query")
    else
        echo "$err" >&2
        refused=error
    fi

    echo "$engine $label refused $refused"
}

function check()
{
    engine=$1
    local table=$2
    local engine_clause=$3
    refused_queries=()

    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS $table SYNC;
        CREATE TABLE $table (k UInt64, a Int64, b Int64, x Int64, c Int64 MATERIALIZED a + b, al Int64 ALIAS a, t Tuple(u Int64))
        ENGINE = $engine_clause ORDER BY k
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
        INSERT INTO $table (k, a, b, x) SELECT number, 1, 1, 0 FROM numbers(4);
    "

    local holder="UPDATE $table SET a = 10, t = tuple(10) WHERE k IN (1, 3)"

    if [[ "$engine" == "MergeTree" ]]
    then
        # Paused after taking the lock, before reading the table.
        arm $FP_PLAIN
        $CLICKHOUSE_CLIENT --query "$SETTINGS $holder" &
        wait_parked $FP_PLAIN
    else
        # Paused after committing the patch, before the lock is released.
        arm $FP_PIPELINE
        $CLICKHOUSE_CLIENT --query_id "${QID_PREFIX}${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}" --query "$SETTINGS $holder" &
        wait_parked $FP_PIPELINE
        local in_progress
        in_progress=$($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.zookeeper
            WHERE path = '/zookeeper/$CLICKHOUSE_DATABASE/$table/lightweight_updates/in_progress' AND startsWith(name, 'update-')")
        if [[ "$in_progress" != 1 ]]
        then
            echo "The holder does not hold the lock in Keeper" >&2
            exit 2
        fi
    fi

    # The other input of `c`.
    waiter W1 "UPDATE $table SET b = 20 WHERE k = 1"
    # Reads `c`, which the holder rewrites.
    waiter W2 "UPDATE $table SET x = c WHERE k = 1"
    # The same input of `c` as the holder.
    waiter W3 "UPDATE $table SET a = 30 WHERE k = 2"
    # Unrelated to the holder.
    waiter W4 "UPDATE $table SET x = 5 WHERE k = 0"
    # Two commands: on row 3, selected only by the second one, the first one writes the stored `a`.
    waiter W5 "ALTER TABLE $table UPDATE a = 40 WHERE k = 2, UPDATE x = 7 WHERE k = 3 SETTINGS alter_update_mode = 'lightweight'"
    # Reads `a` through an ALIAS.
    waiter W6 "UPDATE $table SET x = 6 WHERE k = 0 AND al > 5"
    # Reads `t` through a subcolumn.
    waiter W7 "UPDATE $table SET x = 7 WHERE k = 0 AND t.u > 5"

    if [[ "$engine" == "MergeTree" ]]
    then
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_PLAIN"
    else
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_PIPELINE"
    fi
    wait

    for query in "${refused_queries[@]}"
    do
        $CLICKHOUSE_CLIENT --query "$SETTINGS $query"
    done

    $CLICKHOUSE_CLIENT --query "SELECT k, a, b, x, c FROM $table ORDER BY k SETTINGS apply_patch_parts = 1"
    $CLICKHOUSE_CLIENT --query "DROP TABLE $table SYNC"
}

check MergeTree t_lwu_mat_plain "MergeTree"
check ReplicatedMergeTree t_lwu_mat_rmt "ReplicatedMergeTree('/zookeeper/{database}/t_lwu_mat_rmt/', '1')"
