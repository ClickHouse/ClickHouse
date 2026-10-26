#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A query cancelled by its client over the native protocol must stop a long `sleepEachRow` block (300 s here)
# instead of running it to the end.

settings="max_block_size = 100, max_threads = 1, function_sleep_max_microseconds_per_block = 600000000"

# The query has been running for a while, so it is sleeping.
function wait_for_query()
{
    local where=$1
    local start=$SECONDS
    while (( SECONDS - start < 60 )); do
        [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE $where AND elapsed > 1")" = 1 ] && return
        sleep 0.1
    done
    echo "query did not start: $where"
}

function wait_until_gone()
{
    local name=$1 where=$2
    local start=$SECONDS
    while (( SECONDS - start < 60 )); do
        if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE $where")" = 0 ]; then
            echo "$name: stopped"
            return
        fi
        sleep 0.2
    done
    echo "$name: still running"
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE $where SYNC FORMAT Null"
}

# The remote part of a query whose initiator is killed: the initiator cancels it with a `Cancel` packet.
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_remote"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT count() FROM remote('127.0.0.2', numbers(100)) WHERE sleepEachRow(3) = 0
    SETTINGS prefer_localhost_replica = 0, $settings" >/dev/null 2>&1 &
wait_for_query "initial_query_id = '$query_id' AND NOT is_initial_query AND query LIKE '%sleepEachRow%'"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
wait_until_gone "remote" "initial_query_id = '$query_id'"

# A query with a completed pipeline (INSERT SELECT) cancelled by `clickhouse-client` on SIGINT.
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_insert"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    INSERT INTO FUNCTION null('x UInt8') SELECT sleepEachRow(3) FROM numbers(100)
    SETTINGS $settings" >/dev/null 2>&1 &
client_pid=$!
wait_for_query "query_id = '$query_id'"
kill -INT $client_pid
wait_until_gone "insert select" "query_id = '$query_id'"

# A streamed INSERT: with `parallel_view_processing` the pipeline runs in its own threads, so the server still reads
# from the client while the materialized view sleeps on the first block.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (x UInt8) ENGINE = Memory;
    CREATE TABLE dst (x UInt8) ENGINE = Memory;
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT sleepEachRow(3) + x AS x FROM src"
fifo="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.fifo"
rm -f "$fifo"
mkfifo "$fifo"

function start_streamed_insert()
{
    local query_id=$1
    $CLICKHOUSE_CLIENT --query_id "$query_id" --async_insert 0 --parallel_view_processing 1 --max_threads 4 \
        --max_threads_min_free_memory_per_thread 0 --input_format_parallel_parsing 0 --max_insert_block_size 100 \
        --max_block_size 100 --function_sleep_max_microseconds_per_block 600000000 \
        -q "INSERT INTO src FORMAT TSV" < "$fifo" >/dev/null 2>&1 &
    client_pid=$!
    # Let bash pick the descriptor: under CI, shell_config.sh sends the xtrace to fd 3.
    exec {fifo_fd}>"$fifo"
    seq 150 1>&"${fifo_fd}"
    wait_for_query "query_id = '$query_id' AND read_rows > 0"
}

# After SIGINT, `clickhouse-client` sends a `Cancel` when it reads the next block from stdin.
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_streamed_insert"
start_streamed_insert "$query_id"
kill -INT $client_pid
seq 100 1>&"${fifo_fd}" 2>/dev/null
wait_until_gone "streamed insert" "query_id = '$query_id'"
exec {fifo_fd}>&-

# The client dies, so the server reads the end of the connection instead of a `Cancel`.
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_streamed_insert_killed"
start_streamed_insert "$query_id"
kill -KILL $client_pid
wait $client_pid 2>/dev/null
wait_until_gone "streamed insert, client killed" "query_id = '$query_id'"
exec {fifo_fd}>&-
rm -f "$fifo"

# A completed pipeline that runs inside the query: CREATE TABLE ... AS SELECT in an Atomic database fills the table
# before publishing it. The database is created here, so that a Replicated one does not replay the query under another query id.
ctas_database="${CLICKHOUSE_DATABASE}_ctas"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $ctas_database ENGINE = Atomic"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_ctas"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    CREATE TABLE $ctas_database.t ENGINE = Memory AS SELECT sleepEachRow(3) AS x FROM numbers(100)
    SETTINGS $settings" >/dev/null 2>&1 &
client_pid=$!
wait_for_query "query_id = '$query_id'"
kill -INT $client_pid
wait_until_gone "create as select" "query_id = '$query_id'"

wait
$CLICKHOUSE_CLIENT -q "DROP DATABASE $ctas_database"
