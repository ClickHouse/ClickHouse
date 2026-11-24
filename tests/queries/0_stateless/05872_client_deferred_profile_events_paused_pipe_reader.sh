#!/usr/bin/env bash

# `--print-profile-events` with a delay defers the events into a single block that is flushed only
# after the query has finished. With `--server_logs_file=-` that flush goes to stdout. The lossy
# best-effort discipline (drop the output if the sink does not accept it within a short budget) is
# meant only for the case when the query was interrupted and the sink may be a stuck terminal.
# A query that received no interrupt must keep the ordinary blocking write: if stdout is a pipe that
# is already full and whose reader pauses for longer than that budget, the requested profile events
# must still be delivered once the reader resumes, instead of being silently discarded.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_paused_pipe_reader.fifo"
READER_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_paused_pipe_reader.out"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_paused_pipe_reader.err"

CLIENT=""

cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    wait 2>/dev/null
    rm -f "$FIFO" "$READER_OUT" "$CLIENT_ERR"
}
trap cleanup EXIT

rm -f "$FIFO"
mkfifo "$FIFO"

# Keep both ends of the pipe open: descriptor 3 is what the client gets as its stdout, descriptor 4
# is the read end that is drained only later.
exec 3<>"$FIFO"
exec 4<"$FIFO"

# Fill the pipe completely, so that the very first byte the client writes has to wait for the reader.
# The non-blocking mode is set on a separate open file description, not on the one the client inherits.
yes filler | dd of="$FIFO" oflag=nonblock bs=4096 2>/dev/null

# The delay is longer than the query, so every event is deferred to the post-query flush.
$CLICKHOUSE_LOCAL --server_logs_file=- \
    --print-profile-events --profile-events-delay-ms=600000 \
    --query "SELECT 42 FORMAT Null" >&3 2> "$CLIENT_ERR" &
CLIENT=$!

# The client is now the only writer, so the reader below gets EOF as soon as the client exits.
exec 3>&-

# The reader stays paused for longer than the best-effort budget (one second) and then resumes.
sleep 5
timeout 120 cat <&4 > "$READER_OUT"
exec 4<&-

wait "$CLIENT"
EXIT_CODE=$?
CLIENT=""

if [ "$EXIT_CODE" -eq 0 ]
then
    echo "OK: client finished successfully"
else
    echo "FAIL: client exit code ${EXIT_CODE}"
    cat "$CLIENT_ERR"
fi

if grep -q -a "ContextLock" "$READER_OUT"
then
    echo "OK: the deferred profile events reached the pipe after its reader resumed"
else
    echo "FAIL: the deferred profile events were lost"
    grep -a -v "filler" "$READER_OUT" | head -n 20
    cat "$CLIENT_ERR"
fi
