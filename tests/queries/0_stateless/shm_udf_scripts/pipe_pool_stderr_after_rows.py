#!/usr/bin/python3

# A pooled command that answers its rows and then, immediately, writes a diagnostic to `stderr`.
#
# Configured with `stderr_reaction` `throw`, which promises that anything the command writes to
# `stderr` fails the query. A pooled worker that satisfied the row count is handed straight back to
# the pool without being waited for, so this is the one path where nothing looks at its `stderr`
# again - the probe that refuses to pool a dirty worker runs after the query has already succeeded.
# The setting would then cost the command a worker while telling the user nothing, which is not what
# it says it does.
#
# The diagnostic is put on the stderr pipe *before* the rows are flushed to stdout: the rows are
# composed into the buffer first, the diagnostic goes out, and only then the buffer is flushed. From
# the server's side the diagnostic is therefore already waiting whenever the rows arrive, and what
# it can act on is deterministic. The other order - rows flushed, then the diagnostic - would leave
# a window in which the server has its rows, looks at stderr once, and the diagnostic is still on
# the way; a command that lost the CPU in that window would pass, and the test would flake. Output
# that lands after the server has looked is beyond reach without making every successful pooled
# call wait for output that usually never comes.

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        sys.stdout.write(f"Key {line}\n")

        sys.stderr.write("complaining right after the rows\n")
        sys.stderr.flush()

        sys.stdout.flush()
