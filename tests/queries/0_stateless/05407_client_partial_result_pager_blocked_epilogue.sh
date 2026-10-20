#!/usr/bin/env bash
# Tags: no-fasttest

# With `partial_result_on_first_cancel`, a single Ctrl+C during the query only requests the partial
# result, so the query is never fully cancelled. When a pager is used, finishing it re-installs the
# signal handler, which disarms the query interrupt handler. The received interrupt must still be
# remembered, so that the post-query epilogue (`--processed-rows`) is written with a bounded write
# instead of hanging on a stdout that stopped draining.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PAGER_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_partial_result_pager_blocked_epilogue.out"
trap 'rm -f "$PAGER_OUT"' EXIT

PAGER_OUT="$PAGER_OUT" python3 - <<'PYEOF'
import fcntl
import os
import shlex
import signal
import subprocess
import time

client = shlex.split(os.environ["CLICKHOUSE_CLIENT"])
database = os.environ.get("CLICKHOUSE_DATABASE", "default")
pager_out = os.environ["PAGER_OUT"]
query_id = database + "_partial_result_pager_blocked_epilogue"

# Both stdout and stderr go to a pty whose master is never read. Fill its output queue up front,
# so that every further write to it blocks - including the epilogue. The pager itself drains its
# input into a file, so the partial result is delivered and the pager finishes on its own.
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
    client + [
        "--pager", "cat > " + shlex.quote(pager_out),
        "--partial_result_on_first_cancel", "1",
        "--processed-rows",
        "--query_id", query_id,
        "--query", "SELECT sleep(1) FROM numbers(30) SETTINGS max_block_size = 1, max_threads = 1",
    ],
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
        # A single Ctrl+C while the query is running: it requests the partial result.
        os.kill(proc.pid, signal.SIGINT)
        try:
            proc.wait(timeout=60)
            print("OK: client terminated after the first Ctrl+C")
        except subprocess.TimeoutExpired:
            print("FAIL: client is still waiting after the first Ctrl+C")
finally:
    if proc.poll() is None:
        proc.kill()
        proc.wait()
    os.close(master)
PYEOF
