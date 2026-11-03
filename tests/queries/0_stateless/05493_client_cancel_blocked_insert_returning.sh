#!/usr/bin/env bash
# Tags: no-fasttest

# A single Ctrl+C must cancel `INSERT ... RETURNING` on the server and terminate the client, even when
# the result is written to an output sink that never drains: discarding the output alone would leave
# the client receiving (and the server producing) an unbounded result.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_insert_returning.fifo"
INFILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_insert_returning.tsv"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_insert_returning.err"
QUERY_ID="${CLICKHOUSE_DATABASE}_cancel_blocked_insert_returning"

CLIENT=""
HOLDER=""

cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    [ -n "$HOLDER" ] && kill "$HOLDER" 2>/dev/null
    wait 2>/dev/null
    rm -f "$FIFO" "$INFILE" "$CLIENT_ERR"
}
trap cleanup EXIT

echo 1 > "$INFILE"

# A reader that opens the pipe but never consumes data: the client fills the pipe buffer and then
# blocks writing the result.
mkfifo "$FIFO"
sleep 1000 < "$FIFO" &
HOLDER=$!

$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" \
    --max_block_size 8192 --max_threads 1 --max_memory_usage 0 \
    --max_rows_to_read 0 --max_result_rows 0 --max_result_bytes 0 \
    --query "
    INSERT INTO FUNCTION null('x UInt64')
    RETURNING (SELECT number, repeat('x', 100) FROM numbers(1000000000000))
    FROM INFILE '$INFILE' FORMAT TSV" \
    > "$FIFO" 2> "$CLIENT_ERR" &
CLIENT=$!

started=0
for _ in {0..120}
do
    count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'" 2>/dev/null)
    if [[ "$count" =~ ^[0-9]+$ ]] && [ "$count" -ge 1 ]
    then
        started=1
        break
    fi
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.5
done

if [ "$started" -ne 1 ]
then
    echo "FAIL: the query did not reach the running state"
    cat "$CLIENT_ERR"
    exit 0
fi

# A single Ctrl+C must be enough.
kill -SIGINT "$CLIENT" 2>/dev/null

for _ in {0..150}
do
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.2
done

if kill -0 "$CLIENT" 2>/dev/null
then
    echo "FAIL: client is still running after Ctrl+C"
else
    echo "OK: client terminated after Ctrl+C"
fi
CLIENT=""

# The query must be cancelled on the server as well.
for _ in {0..150}
do
    count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'")
    [ "$count" = "0" ] && break
    sleep 0.2
done

if [ "$count" = "0" ]
then
    echo "OK: the query is cancelled on the server"
else
    echo "FAIL: the query is still running on the server"
    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC FORMAT Null"
fi
