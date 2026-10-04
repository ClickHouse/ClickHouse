#!/usr/bin/python3

# A pooled command that answers its first key, closes its stdout and then stays alive far longer
# than the `command_termination_timeout` it is configured with. A worker that hung up its stdout
# cannot serve anyone else, so it is discarded - and whether its exit code is wanted decides
# whether the request fails or the process is simply signalled once the budget is spent.

import os
import sys
import time

if __name__ == "__main__":
    line = sys.stdin.readline().replace("\n", "")
    sys.stdout.write(line + "\t" + "Key " + line + "\n")
    sys.stdout.flush()
    # `sys.stdout.close()` leaves the descriptor open (the standard streams are opened with
    # `closefd=False`); the server only sees EOF once the descriptor itself is closed.
    os.close(sys.stdout.fileno())

    time.sleep(60)
