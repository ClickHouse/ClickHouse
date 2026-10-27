#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
pid_file="$dir/clickhouse-server.pid"
rm -rf "$dir"
mkdir -p "$dir"

# A stand-in for the server: it locks the pid file and writes its pid there, as `StatusFile` does.
# Its parent execs into `sleep`, which never reaps it, so it stays a zombie after it is killed.
( python3 -c '
import fcntl, os, sys, time
fd = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT)
fcntl.flock(fd, fcntl.LOCK_EX)
os.write(fd, str(os.getpid()).encode())
time.sleep(300)
' "$pid_file" & exec sleep 600 ) &
parent=$!
trap 'kill -9 $parent $(cat "$pid_file" 2>/dev/null) 2>/dev/null; wait $parent 2>/dev/null; rm -rf "$dir"' EXIT
while [ ! -s "$pid_file" ]; do
    clickhouse_test_time_left || exit 1
    sleep 0.1
done
pid=$(cat "$pid_file")

echo "status before stop, running: $("$CLICKHOUSE_BINARY" status --pid-path "$dir" 2>&1 | grep -c 'is running')"
echo "start before stop, already running: $("$CLICKHOUSE_BINARY" start --prefix "$dir" --pid-path "$dir" --user '' 2>&1 | grep -c 'is already running')"

timeout 30 "$CLICKHOUSE_BINARY" stop --pid-path "$dir" --force > "$dir/stop.log" 2>&1
echo "stop: $?"
echo "kill signals sent: $(grep -c 'Sent kill signal' "$dir/stop.log")"

echo "status after stop, running: $("$CLICKHOUSE_BINARY" status --pid-path "$dir" 2>&1 | grep -c 'is running')"
echo "start after stop, already running: $("$CLICKHOUSE_BINARY" start --prefix "$dir" --pid-path "$dir" --user '' 2>&1 | grep -c 'is already running')"
echo "killed process still exists: $(kill -0 "$pid" 2>/dev/null && echo 1 || echo 0)"
