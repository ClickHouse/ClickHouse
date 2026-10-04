#!/usr/bin/python3

# Answers correctly and then takes its time over cleanup: on stdin EOF - the server telling it to
# exit - it stays alive past `command_termination_timeout` and only then exits successfully.
#
# The exit status of a non-pooled command is waited for without a bound, as it always was, so with
# `check_exit_code` such a command still passes. With `check_exit_code` off the server does not
# wait for the status: the command is signalled once the budget is spent.

import os
import sys
import time

if __name__ == "__main__":
    for line in sys.stdin:
        print("Key " + line, end="")
        sys.stdout.flush()

    # End the output, so the server has the whole answer and is only waiting for this process to
    # go. Without this the server is still waiting for rows and hits `command_read_timeout`
    # instead, which is a different failure from the one this script is about.
    os.close(1)

    # Past the function's `command_termination_timeout` (2 seconds).
    time.sleep(4)
    sys.exit(0)
