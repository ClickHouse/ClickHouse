#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Each server process that the watchdog starts, including the one it starts again after the previous one
# died, must be a new image of the binary with its own address space layout, not a copy of the watchdog.

if [ "$(uname)" != "Linux" ]; then
    echo "@@SKIP@@: the watchdog re-executes the server on Linux only"
    exit 0
fi
if [ "$(cat /proc/sys/kernel/randomize_va_space 2>/dev/null)" = "0" ]; then
    echo "@@SKIP@@: address space layout randomization is disabled"
    exit 0
fi

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05460"
rm -rf "$work"
mkdir -p "$work/data"

pids="$work/pids"
: > "$pids"

cleanup() {
    # The watchdog comes first, and its child dies with it by the parent-death signal.
    while read -r p; do
        [ -n "$p" ] && kill -9 "$p" 2>/dev/null
    done < "$pids"
    wait 2>/dev/null
    rm -rf "$work"
}
trap cleanup EXIT

# The start address of the first mapping of the executable file of a process.
exe_base() {
    local exe
    exe=$(readlink "/proc/$1/exe") || return 1
    awk -v exe=" $exe" 'substr($0, length($0) - length(exe) + 1) == exe { split($1, a, "-"); print a[1]; exit }' "/proc/$1/maps" 2>/dev/null
}

# The child that the watchdog started, other than the one given.
wait_for_child() {
    local child children
    for _ in {1..1200}; do
        kill -0 "$watchdog" 2>/dev/null || return 1
        children=()
        read -r -a children 2>/dev/null < "/proc/$watchdog/task/$watchdog/children"
        for child in "${children[@]}"; do
            [ "$child" != "$1" ] && { echo "$child"; return 0; }
        done
        sleep 0.1
    done
    return 1
}

# Waits until the server process answers a query, and prints the port it listens on.
wait_for_server() {
    local port
    for _ in {1..1200}; do
        kill -0 "$1" 2>/dev/null || return 1
        port=$(lsof -n -a -P -i tcp -s tcp:LISTEN -p "$1" 2>/dev/null | awk -F'[ :]' '/LISTEN/ { print $(NF-1); exit }')
        if [ -n "$port" ] && [ "$($CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$port" -q 'SELECT 1' 2>/dev/null)" = "1" ]; then
            echo "$port"
            return 0
        fi
        sleep 0.1
    done
    return 1
}

cat > "$work/config.xml" <<EOF
<clickhouse>
    <logger><level>information</level><log>$work/server.log</log><console>0</console></logger>
    <path>$work/data/</path>
    <tmp_path>$work/data/tmp/</tmp_path>
    <user_files_path>$work/data/user_files/</user_files_path>
    <users><default><password></password><profile>default</profile><quota>default</quota>
        <networks><ip>::/0</ip></networks></default></users>
    <profiles><default></default></profiles>
    <quotas><default></default></quotas>
</clickhouse>
EOF

# With --daemon, as `clickhouse start` runs it: the restart below also requires that the re-executed server did not detach from the watchdog.
(
    ulimit -c 0
    CLICKHOUSE_WATCHDOG_ENABLE=1 CLICKHOUSE_WATCHDOG_RESTART=1 $CLICKHOUSE_BINARY server --config-file="$work/config.xml" \
        --pid-file="$work/server.pid" --daemon -- --tcp_port=0 --listen_host=127.0.0.1 >/dev/null 2>&1
) 2>/dev/null

for _ in {1..1200}; do
    [ -s "$work/server.pid" ] && break
    sleep 0.1
done
[ -s "$work/server.pid" ] || { echo "the server did not write its pid file"; exit 1; }
child1=$(cat "$work/server.pid")
watchdog=$(awk '/^PPid:/ { print $2 }' "/proc/$child1/status" 2>/dev/null)

# Never signal a parent that is not the watchdog, which runs the same binary.
if [ -z "$watchdog" ] || [ "$(readlink "/proc/$watchdog/exe" 2>/dev/null)" != "$(readlink "/proc/$child1/exe" 2>/dev/null)" ]; then
    echo "$child1" >> "$pids"
    echo "the server is not a child of the watchdog"
    exit 1
fi
echo "$watchdog" >> "$pids"
echo "$child1" >> "$pids"

if [ ! -e "/proc/$watchdog/task/$watchdog/children" ]; then
    echo "@@SKIP@@: /proc does not show the children of a process"
    exit 0
fi

watchdog_exe=$(readlink "/proc/$watchdog/exe")
watchdog_base=$(exe_base "$watchdog")
if [ -z "$watchdog_base" ]; then
    echo "@@SKIP@@: /proc does not show the mappings of a process"
    exit 0
fi

# Two separately executed processes share the layout when randomization does not apply to this binary,
# for example under `setarch -R` or a sanitizer that disables it.
( $CLICKHOUSE_LOCAL -q "SELECT sleep(3)" >/dev/null 2>&1 & echo $! > "$work/local.pid" ) 2>/dev/null
local_pid=$(cat "$work/local.pid")
echo "$local_pid" >> "$pids"
local_base=""
for _ in {1..100}; do
    [ "$(readlink "/proc/$local_pid/exe")" = "$watchdog_exe" ] && local_base=$(exe_base "$local_pid")
    [ -n "$local_base" ] && break
    sleep 0.1
done
if [ "$local_base" = "$watchdog_base" ]; then
    echo "@@SKIP@@: the executable is mapped at the same address in every process"
    exit 0
fi

wait_for_server "$child1" > /dev/null || { echo "the server did not start"; exit 1; }
child1_base=$(exe_base "$child1")

# The watchdog starts the server again after it was killed, as it does after a crash.
kill -KILL "$child1"
child2=$(wait_for_child "$child1") || { echo "the watchdog did not restart the server"; exit 1; }
echo "$child2" >> "$pids"
port2=$(wait_for_server "$child2")
child2_base=$(exe_base "$child2")

echo "first server process has its own layout: $([ -n "$child1_base" ] && [ "$child1_base" != "$watchdog_base" ] && echo 1 || echo 0)"
echo "restarted server process has its own layout: $([ -n "$child2_base" ] && [ "$child2_base" != "$child1_base" ] && [ "$child2_base" != "$watchdog_base" ] && echo 1 || echo 0)"
echo "restarted server answers: $([ -n "$port2" ] && echo 1 || echo 0)"
echo "restarted server process name: $([ "$(cat "/proc/$child2/comm")" = "$(basename "$CLICKHOUSE_BINARY" | head -c 15)" ] && echo 1 || echo 0)"
