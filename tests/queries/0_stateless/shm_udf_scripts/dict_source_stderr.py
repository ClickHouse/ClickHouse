#!/usr/bin/python3

# A source that produces its rows and writes a diagnostic to `stderr`. The diagnostic is put on the
# pipe before the rows are flushed, so it is already waiting whenever the server has the rows and
# what the server does about it does not depend on scheduling.

import sys

if __name__ == "__main__":
    print("1" + "\t" + "Value 1", end="\n")
    print("2" + "\t" + "Value 2", end="\n")
    print("3" + "\t" + "Value 3", end="\n")

    sys.stderr.write("the source complains\n")
    sys.stderr.flush()

    sys.stdout.flush()
