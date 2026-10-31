#!/usr/bin/env bash
# Tags: no-fasttest

# The `clickhouse-local` twin of 05404_client_cancel_outfile_open_blocked_stderr. In a default
# (non-interactive) `clickhouse-local --query` invocation the query exception is rethrown and
# reported only at the top level, after the interrupt handler has been stopped. With `stderr` being
# a terminal that stopped draining, that report must be written with a bounded write too, instead
# of hanging the process after the first Ctrl+C abandoned the blocked `open()` of the FIFO.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_local_outfile_open_blocked_stderr.fifo"
trap 'rm -f "$FIFO"' EXIT

# `APPEND` makes `clickhouse-local` open the pre-created FIFO directly. No reader is ever attached.
mkfifo "$FIFO"

FIFO="$FIFO" python3 - <<'PYEOF'
import fcntl
import os
import shlex
import signal
import subprocess
import time

local = shlex.split(os.environ["CLICKHOUSE_LOCAL"])
fifo = os.environ["FIFO"]

query = (
    "SELECT sleepEachRow(0.1) FROM numbers(1000000) "
    f"INTO OUTFILE '{fifo}' APPEND FORMAT TabSeparated "
    "SETTINGS max_block_size = 1, function_sleep_max_microseconds_per_block = 0, "
    "max_threads = 1, max_rows_to_read = 0, max_result_rows = 0"
)

# Both stdout and stderr go to a pty whose master is never read. Fill its output queue up front,
# so that every further write to it blocks - including the report of the exception.
master, slave = os.openpty()
flags = fcntl.fcntl(slave, fcntl.F_GETFL)
fcntl.fcntl(slave, fcntl.F_SETFL, flags | os.O_NONBLOCK)
filler = b"." * 4096
try:
    while True:
        os.write(slave, filler)
except BlockingIOError:
    pass
fcntl.fcntl(slave, fcntl.F_SETFL, flags)

proc = subprocess.Popen(
    local + ["--query", query],
    stdin=subprocess.DEVNULL,
    stdout=slave,
    stderr=slave,
    close_fds=True,
)
os.close(slave)

try:
    # There is no server to ask whether the query is running, and the blocked `open()` has no
    # observable side effect, so give the process ample time to start the query and park in it.
    time.sleep(3)
    os.kill(proc.pid, signal.SIGINT)
    try:
        proc.wait(timeout=30)
        print("OK: clickhouse-local terminated after the first Ctrl+C")
    except subprocess.TimeoutExpired:
        print("FAIL: clickhouse-local is still waiting after the first Ctrl+C")
finally:
    if proc.poll() is None:
        proc.kill()
        proc.wait()
    os.close(master)
PYEOF
