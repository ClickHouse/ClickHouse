#!/usr/bin/python3

# Answers with the capacities of the pipes it was started with - `stdin`, `stdout` and `stderr`, as
# `F_GETPIPE_SZ` reports them, separated by spaces - so that a test can see what `command_pipe_capacity`
# made of them. As the source of an executable dictionary it produces one row for key 1; with `--pool`
# it answers every key it is given, as an `executable_pool` worker does, until its stdin is closed.
#
# With `--probe` it runs no protocol and prints a capacity this host lets a pipe grow to: twice the
# default capacity of a new pipe - which is 16 pages, or one page for a user who has reached
# `/proc/sys/fs/pipe-user-pages-soft` - or `0` if that is above `/proc/sys/fs/pipe-max-size` or the
# kernel refuses it, as it does for a user who has reached the limit. See pipe(7).

import fcntl
import os
import sys

# `fcntl.F_GETPIPE_SZ` and `fcntl.F_SETPIPE_SZ` only exist from Python 3.10 on.
F_SETPIPE_SZ = getattr(fcntl, "F_SETPIPE_SZ", 1031)
F_GETPIPE_SZ = getattr(fcntl, "F_GETPIPE_SZ", 1032)


def capacities():
    return " ".join(str(fcntl.fcntl(fd, F_GETPIPE_SZ)) for fd in (0, 1, 2))


def probe():
    read_end, write_end = os.pipe()
    try:
        target = 2 * fcntl.fcntl(write_end, F_GETPIPE_SZ)
        with open("/proc/sys/fs/pipe-max-size") as pipe_max_size:
            if target > int(pipe_max_size.read()):
                return 0
        try:
            fcntl.fcntl(write_end, F_SETPIPE_SZ, target)
        except PermissionError:
            return 0
        return target
    finally:
        os.close(read_end)
        os.close(write_end)


if __name__ == "__main__":
    if "--probe" in sys.argv[1:]:
        print(probe())
    elif "--pool" in sys.argv[1:]:
        for line in sys.stdin:
            print(f"{line.strip()}\t{capacities()}")
            sys.stdout.flush()
    else:
        print(f"1\t{capacities()}")
