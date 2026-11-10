#!/usr/bin/env bash

# Waits, from inside a `clickhouse-local` session, for a state of a command's process that nothing
# in SQL can observe, so that the next query starts only once that state is provably reached rather
# than after a sleep long enough to hope for it. Runs as an `executable` table function: reads the
# pid of the process from stdin and prints `1` once the state is there, or `0` if it is not reached
# within a few seconds - under the table function's own read timeout.
#
#   shm_wait.sh blocked        - the process is blocked in `write` on a full pipe nobody reads
#   shm_wait.sh exited         - the process has exited (a zombie for its parent to reap, or gone)
#   shm_wait.sh reaped         - the process is gone altogether: exited and reaped, not a zombie
#   shm_wait.sh file <path>    - the file exists; it is removed once seen, so that a command that
#                                creates it again can be waited for again (the pid on stdin is not used)
#   shm_wait.sh touch <path>   - not a wait: creates the file - the signal a command waits for in
#                                `go_signal.py` - and prints `1` (the pid on stdin is not used)

read -r pid

if [[ "$1" == touch ]]; then
    touch "$2" && echo 1
    exit 0
fi

for _ in $(seq 1 160); do
    case "$1" in
        blocked)
            # `pipe_write` up to Linux 6.x, `anon_pipe_write` from 7.0 on.
            [[ "$(cat /proc/"$pid"/wchan 2>/dev/null)" == *pipe_write ]] && echo 1 && exit 0
            ;;
        exited)
            stat=$(cat /proc/"$pid"/stat 2>/dev/null)
            [[ -z "$stat" || "${stat##*) }" == Z* ]] && echo 1 && exit 0
            ;;
        reaped)
            [[ ! -e /proc/"$pid" ]] && echo 1 && exit 0
            ;;
        file)
            rm "$2" 2>/dev/null && echo 1 && exit 0
            ;;
    esac
    sleep 0.05
done

echo 0
