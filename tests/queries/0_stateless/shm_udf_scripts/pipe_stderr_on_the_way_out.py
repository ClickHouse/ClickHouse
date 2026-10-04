#!/usr/bin/python3

# Answers correctly, closes its stdout, waits long enough for the server to give up draining, and
# only then writes its diagnostic to stderr before exiting.
#
# The pause is the point. The drain that runs when stdout ends stops as soon as stderr goes quiet
# for a moment, so a line written well after that is found only by the bounded wait that reaps the
# command - the last stretch in which a command can write anything at all. Those bytes are still
# output the command produced, and `stderr_reaction` `throw` promises that output fails the query.
# That promise has nothing to do with `check_exit_code`, so it has to hold with the exit-status
# check switched off too.

import os
import sys
import time

if __name__ == "__main__":
    for line in sys.stdin:
        print("Key " + line, end="")
        sys.stdout.flush()

    # End the output, then outlast the drain's idle window (100 ms) by a wide margin.
    os.close(1)
    time.sleep(1)

    sys.stderr.write("complaining on the way out\n")
    sys.stderr.flush()

    sys.exit(0)
