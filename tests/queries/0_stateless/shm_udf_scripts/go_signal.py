# What the commands of the executable UDF tests do instead of sleeping before their late output.
#
# A command that is to write something after its answer - once the query it answered is over, hand-
# back probe and all - cannot know when that is: a sleep long enough on one machine is too short on a
# loaded one, and then the output lands in the probe of the query it was meant to miss. So the test
# says when, from the next SQL statement, by creating the file named by `--go` (`shm_wait.sh touch`);
# the statement before it has returned by then, and with it the hand-back of the worker. The file is
# consumed, so that every late write takes a signal of its own and a replacement worker does not
# write on a stale one.


import os
import select
import sys


def wait_for_go():
    """Waits for the `--go` file and consumes it. Returns False, without waiting further, as soon as
    there is something on stdin instead - the next request, or the end of it: a command that never
    gets its signal must neither miss a request nor sit out `command_termination_timeout` when the
    server lets it go."""
    path = sys.argv[sys.argv.index("--go") + 1]
    while True:
        try:
            os.remove(path)
            return True
        except FileNotFoundError:
            pass
        if select.select([sys.stdin.fileno()], [], [], 0.02)[0]:
            return False
