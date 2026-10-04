#!/usr/bin/python3

# A command that answers, closes its stdout and exits a moment later - the moment in which it is
# not yet a zombie when the server first looks for its exit status. With a termination timeout of
# zero that look must not be the only one.

import os
import sys
import time

if __name__ == "__main__":
    for line in sys.stdin:
        if not line.strip():
            continue
        os.write(1, ("Key " + line.strip() + "\n").encode())
    os.close(1)
    time.sleep(0.3)
    sys.exit(0)
