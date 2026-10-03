#!/usr/bin/env bash

# Waits, from inside a `clickhouse-local` session, for a state of a command's process that nothing
# in SQL can observe, so that the next query starts only once that state is provably reached rather
# than after a sleep long enough to hope for it. Runs as an `executable` table function: reads the
# pid of the process from stdin and prints `1` once the state is there, or `0` if it is not reached
# within a few seconds - under the table function's own read timeout.
#
#   shm_wait.sh blocked        - the process is blocked in `write` on a full pipe nobody reads
#   shm_wait.sh exited         - the process has exited (a zombie for its parent to reap, or gone)
#   shm_wait.sh file <path>    - the file exists (the pid on stdin is not used)

read -r pid

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
        file)
            [[ -e "$2" ]] && echo 1 && exit 0
            ;;
    esac
    sleep 0.05
done

echo 0
