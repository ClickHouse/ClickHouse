#!/usr/bin/python3

# A pooled shared-memory UDF (see `shm_protocol.py`) whose process tree outlives its own discarding
# unless the server ends it. At startup it forks a descendant that keeps the inherited descriptor of
# the region (and nothing else), ignores `SIGTERM` and sleeps. It answers its request with a stray
# byte after the response frame - in the same write, so the server's hand-back probe is sure to see
# it and discard the worker - and when its stdin reaches EOF it does not exit either: it ignores
# `SIGTERM` and sleeps. Both write their pids, as `worker` and `descendant`, into the directory
# named by the argument. Either of them alive after the server dropped the region could still write
# into it.

import os
import signal
import sys
import time

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import answer_rows, key_row, serve  # noqa: E402

REGION_FD = 3


def write_pid(directory, name):
    with open(os.path.join(directory, name + ".tmp"), "w") as out:
        out.write(f"{os.getpid()}\n")
    os.rename(os.path.join(directory, name + ".tmp"), os.path.join(directory, name))


def start_descendant(directory):
    if os.fork() != 0:
        return
    # The descendant: the region's descriptor and nothing else, deaf to `SIGTERM`.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    os.closerange(0, REGION_FD)
    os.closerange(REGION_FD + 1, 1024)
    write_pid(directory, "descendant")
    time.sleep(60)
    os._exit(0)


def wait_for(directory, name):
    while not os.path.exists(os.path.join(directory, name)):
        time.sleep(0.01)


def linger():
    # Discarded: the server closed stdin. Stay, deaf to `SIGTERM`, holding the region.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(60)


def main():
    start_descendant(sys.argv[1])
    # The descendant has written its pid before the first answer: the server may discard this worker,
    # and end its group, right after it.
    wait_for(sys.argv[1], "descendant")
    write_pid(sys.argv[1], "worker")

    # Every response frame with a stray byte after it, in the same write (see `serve`).
    serve(answer_rows(key_row), after_frame=b"\n")
    linger()


if __name__ == "__main__":
    main()
