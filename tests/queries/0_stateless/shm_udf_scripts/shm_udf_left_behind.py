#!/usr/bin/python3

# A shared-memory UDF (see `shm_protocol.py`) that leaves processes behind when it exits. At startup
# it forks two descendants that keep the inherited descriptor of the region (and nothing else): one
# stays in the command's process group and sleeps; the other leaves the group (`setsid`), waits for
# a file `go` to appear in the directory named by the argument, then writes the number of blocks the
# region's file holds into `left_blocks` there and exits. The command itself answers normally and
# exits when its stdin reaches EOF. The pids go into the same directory, as `worker`, `in_group` and
# `left_group`.

import os
import sys
import time

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import answer_rows, key_row, serve  # noqa: E402

REGION_FD = 3


def write_file(directory, name, text):
    with open(os.path.join(directory, name + ".tmp"), "w") as out:
        out.write(text)
    os.rename(os.path.join(directory, name + ".tmp"), os.path.join(directory, name))


def write_pid(directory, name):
    write_file(directory, name, f"{os.getpid()}\n")


def keep_only_the_region():
    os.closerange(0, REGION_FD)
    os.closerange(REGION_FD + 1, 1024)


def start_in_group(directory):
    if os.fork() != 0:
        return
    keep_only_the_region()
    write_pid(directory, "in_group")
    time.sleep(60)
    os._exit(0)


def start_left_group(directory):
    if os.fork() != 0:
        return
    os.setsid()
    keep_only_the_region()
    write_pid(directory, "left_group")
    deadline = time.monotonic() + 60
    while not os.path.exists(os.path.join(directory, "go")) and time.monotonic() < deadline:
        time.sleep(0.05)
    write_file(directory, "left_blocks", f"{os.fstat(REGION_FD).st_blocks}\n")
    os._exit(0)


def wait_for(directory, name):
    while not os.path.exists(os.path.join(directory, name)):
        time.sleep(0.01)


def main():
    directory = sys.argv[1]
    start_in_group(directory)
    start_left_group(directory)
    # Both descendants are where they belong (`left_group` out of the group) before the first answer.
    wait_for(directory, "in_group")
    wait_for(directory, "left_group")
    write_pid(directory, "worker")

    serve(answer_rows(key_row))


if __name__ == "__main__":
    main()
