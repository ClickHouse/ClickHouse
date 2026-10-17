#!/usr/bin/env bash
# Tags: no-fasttest

# The first Ctrl+C abandons the blocked `open()` of an `INTO OUTFILE` FIFO that never gets a reader,
# and the query ends with `QUERY_WAS_CANCELLED`. The client then reports that exception on `stderr`
# after the interrupt handler has already been stopped. Here `stderr` is a terminal that stopped
# draining (as it is on an interactive terminal stuck together with the output), so the report must
# be written with a bounded write instead of hanging the client.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_outfile_open_blocked_stderr.fifo"
trap 'rm -f "$FIFO"' EXIT

# `APPEND` makes the client open the pre-created FIFO directly.
mkfifo "$FIFO"

FIFO="$FIFO" python3 - <<'PYEOF'
import fcntl
import os
import shlex
import signal
import subprocess
import time

client = shlex.split(os.environ["CLICKHOUSE_CLIENT"])
database = os.environ.get("CLICKHOUSE_DATABASE", "default")
fifo = os.environ["FIFO"]
query_id = database + "_cancel_outfile_open_blocked_stderr"

query = (
    "SELECT number FROM numbers(1000000000) "
    f"INTO OUTFILE '{fifo}' APPEND FORMAT TabSeparated "
    "SETTINGS max_block_size = 8192, max_threads = 1, max_rows_to_read = 0, "
    "max_result_rows = 0, max_result_bytes = 0"
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
    client + ["--query_id", query_id, "--query", query],
    stdin=subprocess.DEVNULL,
    stdout=slave,
    stderr=slave,
    close_fds=True,
)
os.close(slave)


def query_running():
    out = subprocess.run(
        client + ["--query", f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"],
        capture_output=True,
        text=True,
    ).stdout.strip()
    return out.isdigit() and int(out) >= 1


try:
    # Once the query is running, the client has received the first block and is blocked opening
    # the output file.
    started = False
    for _ in range(120):
        if query_running():
            started = True
            break
        if proc.poll() is not None:
            break
        time.sleep(0.5)

    if not started:
        print("FAIL: the query did not reach the running state")
    else:
        os.kill(proc.pid, signal.SIGINT)
        try:
            proc.wait(timeout=30)
            print("OK: client terminated after the first Ctrl+C")
        except subprocess.TimeoutExpired:
            print("FAIL: client is still waiting after the first Ctrl+C")
finally:
    if proc.poll() is None:
        proc.kill()
        proc.wait()
    os.close(master)
PYEOF
