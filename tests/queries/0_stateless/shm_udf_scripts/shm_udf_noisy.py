#!/usr/bin/python3

# A shared-memory UDF (see `shm_protocol.py`) that answers every request correctly - with `Key <row>`,
# or with its own pid (`--report-pid`), so that a test can tell a reused worker from a fresh one - and
# writes something besides its response frames: to stderr, or past the frame on its stdout, before the
# answer, with it, or after it. What, and when, is what the flags say; a late write waits until the
# query it answered is over when `--go` is given (see `go_signal.py`).

import argparse
import os
import sys
import threading
import time

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from go_signal import wait_for_go  # noqa: E402
from shm_protocol import answer_rows, key_row, pid_row, read_request, respond  # noqa: E402

# Comfortably past the 64 KiB a Linux pipe holds by default, so the write cannot finish unless
# somebody reads the pipe.
PIPES_WORTH = 4 * 1024 * 1024
# Twice the 64 KiB a Linux pipe holds by default, so the command is provably blocked partway.
TWO_PIPEFULS = 128 * 1024
# Enough to be several times the 10 ms tick that `/proc/<pid>/stat` counts CPU in.
CPU_ITERATIONS = 400000

stdout = sys.stdout.buffer
stderr = sys.stderr.buffer


def write_stderr(data):
    stderr.write(data)
    stderr.flush()


# What goes to stderr after the answer is computed and before the response frame is sent.
STDERR_BEFORE_FRAME = {
    # A line with every answer. It is on the pipe before the server has the whole frame, and the
    # server, which polls stderr together with stdout while it waits for the response, takes it off
    # the pipe within the same invocation - so it is attributed to the query that caused it, whatever
    # the reaction is, and never to the next one.
    #
    # A line written after the frame is a different case, and not one a test can pin down: once the
    # server has the response it no longer waits for the command, so whether the line is in time for
    # the check before the worker goes back to the pool is up to the scheduler. A line that misses
    # that check is found by the next borrow, which is tested with a command that waits for it
    # (`--go --after-answer stderr-flood`).
    "line": b"done\n",
    # Far more diagnostics than a pipe can hold, before the answer, which is where diagnostics belong
    # - and where they deadlock a server that is waiting for a response it will never get. Configured
    # with `stderr_reaction` `none`: "none" says what to do with those bytes - nothing - not that the
    # pipe may be left unread. A server that stops polling stderr because it has no use for it lets
    # the pipe fill up, and the command then blocks in `write` with its answer unwritten: the request
    # times out, and for a pooled process the leftovers carry over into whichever query borrows it
    # next. So the bytes have to be read and discarded, and this command has to be answered normally,
    # every time, on the same worker.
    "flood": b"d" * PIPES_WORTH,
}

# What goes to stdout past every response frame, in the same write (see `serve`).
STDOUT_AFTER_FRAME = {
    # One byte: the accident a stray `print` or a forgotten newline is. Nothing in the exchange
    # notices it, because the answer itself was read in full and the result lives in the
    # shared-memory region; the byte is only ever read by the *next* request on the same process, as
    # the status varint of a response that has not been sent yet. The server must therefore refuse to
    # return this worker to the pool, so the damage stays inside the invocation that caused it.
    "byte": b"\n",
    # Far more than a pipe can hold: the same accident, in the version that also traps the server.
    # Nothing reads that pipe once the response frame has been taken off it, so the child stays stuck
    # in `write`. Closing its stdin - which is what discarding a worker means - does not help: a
    # process blocked in `write` is not waiting for input. A server that reaps such a child with a
    # plain blocking `waitpid` never returns from it, and the query hangs with its result already
    # computed. The bytes have to be taken off the pipe and thrown away so the child can reach its own
    # exit - a clean one, which `check_exit_code` must be able to inspect - and the wait has to be
    # bounded either way.
    "flood": b"x" * PIPES_WORTH,
}


def stderr_flood():
    # Answers, then writes two pipefuls to stderr before going back to read the next request.
    #
    # Configured with `stderr_reaction` `none`, which documents that a chatty command never blocks on
    # a full stderr pipe. That promise is easy to keep while a query is running - the read loop drains
    # both pipes - and easy to lose at the moment the worker is handed back to the pool: nothing is
    # reading it any more, `none` means the bytes are nobody's, and a worker returned with a full pipe
    # is a worker blocked in `write` that will never read the next request.
    #
    # With `--go` it stays quiet until the query it answered is over, hand-back drain and all. A drain
    # at the moment the worker is handed back then sees an empty pipe and can say nothing about what
    # the command is going to write next; what actually keeps the promise of `none` is that the read
    # loop of the *next* borrow polls stderr alongside stdout and keeps taking bytes off it while it
    # waits for the response, so a command blocked in `write` is unblocked by the very query that is
    # waiting for it.
    if "--go" not in sys.argv or wait_for_go():
        write_stderr(b"e" * TWO_PIPEFULS)


def last_words():
    # Once the query just served is over (`--go`), complain and go: the worker dies in the pool with
    # its last words unread, and the next borrow has to report them. See `pipe_pool_pid.py
    # --after-go last-words` for the pipe counterpart.
    if not wait_for_go():
        sys.exit(0)
    write_stderr(b"last words of the worker\n")
    os._exit(0)


def stray_byte():
    # Once the server has taken this worker back and pronounced it clean (`--go`), one stray byte on
    # stdout. The file named by `--marker` is created after the byte is on the pipe, so a test that
    # waits for it does not have to guess how long this takes on the machine it runs on.
    #
    # The server probes the worker's pipes when it takes it back and refuses to pool one that left
    # anything behind - but a probe is one instant, and this byte arrives after it. The worker goes
    # back into the pool looking clean, and the byte is waiting there for whoever borrows it next.
    #
    # What that byte costs depends entirely on whether the protocol can tell one answer from another.
    # As a bare status varint it is a plausible frame: `0` reads as success, the real status becomes
    # the offset, the real offset becomes the size - and with a compatible format the next query gets
    # the region's own *input* back as its result, in the right number of rows, with no error
    # anywhere. Two things make that impossible. The next borrow looks at the worker's stdout before
    # it sends anything: a byte found there is provably not the new query's, so the worker is
    # discarded and replaced, and the query is answered by the replacement. And a byte that lands
    # between that look and the request is caught by the request id: the frame carries the wrong id,
    # and the query fails loudly instead of answering wrongly.
    def litter():
        if not wait_for_go():
            return
        stdout.write(b"\x00")
        stdout.flush()
        with open(sys.argv[sys.argv.index("--marker") + 1], "w"):
            pass

    threading.Thread(target=litter, daemon=True).start()


def stderr_then_exit():
    # Complains at length and exits in the same breath - no pause anywhere. This is the timing the
    # other late-stderr scenarios deliberately avoid, and the one that catches a server which reaps
    # before it reads: reaping closes the child's pipes, and everything the child had written and
    # nobody had read yet goes with them. Under `stderr_reaction` `throw` the query would then succeed
    # while the command was shouting.
    #
    # The size is chosen deliberately: comfortably more than one 4 KiB read, so picking it up takes
    # several, and comfortably less than a pipeful, so the command is never blocked in `write` and can
    # really exit in the same breath. A message larger than the pipe would block the writer, keep the
    # process alive, and quietly turn this into a test of something else.
    write_stderr(b"e" * 8192)
    os._exit(0)


def stderr_on_the_way_out():
    # Closes stdout, waits long enough for the server to give up draining, and only then writes its
    # diagnostic to stderr before exiting.
    #
    # The pause is the whole point. The drain that runs when stdout ends stops as soon as stderr goes
    # quiet for a moment (100 ms), so a line written well after that is not found there - it is found
    # by the bounded wait that reaps the command, which is the last stretch in which a command can
    # write anything at all. Those bytes are still output the command produced, and `stderr_reaction`
    # `throw` promises that output fails the query. Read and dropped on the floor, the query would
    # succeed and the setting would quietly mean nothing on the way out.
    os.close(1)
    time.sleep(1)
    write_stderr(b"complaining on the way out\n")
    sys.exit(0)


def stderr_flood_on_the_way_out():
    # Closes stdout and only then writes far more to stderr than a pipe can hold - the shape a command
    # has when it dumps a summary on its way out.
    #
    # Configured with `stderr_reaction` `none`. Nobody is reading stderr any more once stdout has
    # ended, so the command blocks in `write` and never reaches its own exit. A server that then reaps
    # it with a blocking `waitpid` waits for a process that is waiting for the server, and the query
    # hangs with its result already computed. Both halves have to hold: the rest of stderr is taken off
    # the pipe when stdout ends, whatever the reaction, and the wait that reaps the command is bounded
    # and keeps draining while it waits.
    os.close(1)
    write_stderr(b"e" * PIPES_WORTH)
    sys.exit(0)


AFTER_ANSWER = {
    "stderr-flood": stderr_flood,
    "last-words": last_words,
    "stray-byte": stray_byte,
    "stderr-then-exit": stderr_then_exit,
    "stderr-on-the-way-out": stderr_on_the_way_out,
    "stderr-flood-on-the-way-out": stderr_flood_on_the_way_out,
}

parser = argparse.ArgumentParser()
parser.add_argument("--report-pid", action="store_true")
# Closes its own stderr once it is up, before answering anything, so every borrow sees a stderr that
# is closed and will stay closed. Commands do this - a script that daemonizes its logging, or simply
# does not want the descriptor - and it is entirely legal: the protocol lives on stdin/stdout and the
# region. Once the only writer closes it, stderr polls as `POLLHUP` forever, with no `POLLIN` and
# nothing to read. A reuse check that asked only "is anything pending?" would read that hangup as
# leftover output and throw this worker away on every single borrow, turning `executable_pool` into a
# process per call - quietly, because the queries themselves would all still succeed.
parser.add_argument("--close-stderr", action="store_true")
# Burns a measurable amount of CPU answering. With `--stdout-after-frame byte` the server discards the
# worker, and that discard is what this is for: the borrow's CPU and peak resident set are read out of
# `/proc/<pid>`, so they have to be read while the pid is still there - closing the child's stdin makes
# it exit, and a zombie has no `VmHWM` left, while the wait that follows reaps the pid altogether. A
# server that samples after any of that reports zero for exactly the borrows whose accounting matters
# most - the ones that went wrong.
parser.add_argument("--burn-cpu", action="store_true")
parser.add_argument("--stderr-before-frame", choices=STDERR_BEFORE_FRAME)
parser.add_argument("--stdout-after-frame", choices=STDOUT_AFTER_FRAME)
# What the command does once it has answered; the last three end the process after the first answer.
parser.add_argument("--after-answer", choices=AFTER_ANSWER)
parser.add_argument("--go", metavar="PATH")
parser.add_argument("--marker", metavar="PATH")
args = parser.parse_args()


def burn_cpu():
    acc = 0
    for i in range(CPU_ITERATIONS):
        acc = (acc + i * i) % 1000003
    return acc


def main():
    stdin = sys.stdin.buffer
    answer = answer_rows(pid_row if args.report_pid else key_row)

    if args.close_stderr:
        os.close(2)

    while True:
        request = read_request(stdin)
        if request is None:
            break  # stdin closed -> exit

        if args.burn_cpu:
            burn_cpu()
        frame = respond(request, answer)

        if args.stderr_before_frame:
            write_stderr(STDERR_BEFORE_FRAME[args.stderr_before_frame])

        stdout.write(frame + STDOUT_AFTER_FRAME.get(args.stdout_after_frame, b""))
        stdout.flush()

        if args.after_answer:
            AFTER_ANSWER[args.after_answer]()


if __name__ == "__main__":
    main()
