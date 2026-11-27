#!/usr/bin/env bash
# Tags: no-fasttest

# With `partial_result_on_first_cancel`, a first Ctrl+C only requests the partial result, and the
# cancelled query still has to be drained. The periodic progress redraws must not starve that drain
# when the progress terminal is stuck: a redraw waiting for the terminal can take as long as the
# period of the progress packets, so `clickhouse-local` would always have the next packet due and
# never advance the cancelled query. The result sink (a pager) keeps draining here, only the
# progress terminal is blocked, and one Ctrl+C must complete the cancellation without draining it.
# The same is checked for the progress table with parallel formatting.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RESULT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_cancel_blocked_progress_terminal.result"
LOCAL_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_cancel_blocked_progress_terminal.err"
trap 'rm -f "$RESULT_FILE" "$LOCAL_ERR"' EXIT

RESULT_FILE="$RESULT_FILE" LOCAL_ERR="$LOCAL_ERR" python3 - <<'PYEOF'
import errno
import os
import shlex
import signal
import subprocess
import sys
import time

local = shlex.split(os.environ["CLICKHOUSE_LOCAL"])
result_file = os.environ["RESULT_FILE"]
local_err = os.environ["LOCAL_ERR"]

query = (
    "SELECT sleepEachRow(0.01) FROM numbers(1000000) "
    "SETTINGS max_block_size = 1, max_threads = 1, max_execution_time = 0"
)

def run(extra_args):
    # Connect stdout to a pty whose master is never read, and fill its buffer up front, so that
    # every progress redraw on it blocks. stderr goes to a regular file, which never blocks.
    master, slave = os.openpty()
    os.set_blocking(slave, False)
    while True:
        try:
            os.write(slave, b"x" * 4096)
        except OSError as e:
            if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                break
            raise
    os.set_blocking(slave, True)

    if os.path.exists(result_file):
        os.remove(result_file)
    err_fd = os.open(local_err, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    proc = subprocess.Popen(
        local + extra_args + [
            "--partial_result_on_first_cancel=1",
            "--pager", "cat > " + shlex.quote(result_file),
            "--query", query,
        ],
        stdin=subprocess.DEVNULL,
        stdout=slave,
        stderr=err_fd,
        close_fds=True,
    )
    os.close(slave)
    os.close(err_fd)

    try:
        def result_size():
            return os.path.getsize(result_file) if os.path.exists(result_file) else 0

        # Wait until the result stops growing: the query produces a row every 10 ms, so a stalled
        # result means that the client is parked on a progress write to the stuck terminal. The
        # result can stall while still empty: a slow build can draw the progress before the first
        # rows are flushed to the pager, and then the progress clear before that flush blocks.
        started = False
        previous = -1
        unchanged = 0
        for _ in range(600):
            if proc.poll() is not None:
                break
            size = result_size()
            unchanged = unchanged + 1 if size == previous else 0
            if unchanged >= 3:
                started = True
                break
            previous = size
            time.sleep(0.5)

        if not started:
            print("FAIL: the client did not block on the progress terminal")
            with open(local_err) as f:
                sys.stdout.write("--- stderr ---\n" + f.read())
            return

        # A single Ctrl+C, which only requests the partial result.
        os.kill(proc.pid, signal.SIGINT)

        try:
            proc.wait(timeout=30)
            print("OK: the cancelled query completed after the first Ctrl+C")
        except subprocess.TimeoutExpired:
            print("FAIL: still running after the first Ctrl+C")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        os.close(master)


run(["--progress=tty"])

# The progress table with parallel formatting: the progress clear before an output flush runs in
# the formatter's collector thread, and must give up on the stuck terminal on the first Ctrl+C even
# while the main thread starts the teardown of the query.
run(["--progress-table=on", "--enable-progress-table-toggle=0", "--output_format_parallel_formatting=1"])
PYEOF
