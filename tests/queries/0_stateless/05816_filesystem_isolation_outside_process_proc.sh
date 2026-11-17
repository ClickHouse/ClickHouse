#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A program the isolated server runs cannot reach the filesystem of the host through the `/proc` entries of a process
# outside of the isolation: here a process of the same user, dumpable and without capabilities, which the kernel lets
# a process of the same user inspect, with a marker file outside of every mount of the server as its working directory
# and an open descriptor. A `proc` of the host mounted inside a visible path, which would be another way to them, stops
# the startup.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="outside process without capabilities: yes
namespaces: PID and mount
seccomp filter of the server: installed
init of the PID namespace: clickhouse-init
seccomp filter of the program: installed
outside process visible: no
direct read: denied
read through /proc/<pid>/root: denied
write through /proc/<pid>/root: denied
read through /proc/<pid>/cwd: denied
read through /proc/<pid>/fd: denied
marker: unchanged
proc of the host inside path: refused"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
# The supervisor and the new PID namespace are used with `CAP_SYS_ADMIN` (bit 21), as the server started here has it.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ] \
    || ! (( (16#$(awk '$1 == "CapEff:" { print $2 }' /proc/self/status) >> 21) & 1 )); then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_outside_process_proc"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data/user_scripts" "$DIR/log" "$DIR/outside"
DIR=$(cd "$DIR" && pwd -P)
MARKER="$DIR/outside/marker"
echo "outside" > "$MARKER"

cat > "$DIR/data/user_scripts/probe.sh" <<'EOF'
#!/bin/bash
read -r pid marker
result() { if "$@" >/dev/null 2>&1; then echo allowed; else echo denied; fi; }
echo "init of the PID namespace: $(cat /proc/1/comm)"
echo "seccomp filter of the program: $(grep -qE '^Seccomp:[[:space:]]+2$' /proc/self/status && echo installed || echo absent)"
echo "outside process visible: $([ -e "/proc/$pid/cmdline" ] && grep -q sleep "/proc/$pid/cmdline" && echo yes || echo no)"
echo "direct read: $(result cat "$marker")"
echo "read through /proc/<pid>/root: $(result cat "/proc/$pid/root$marker")"
echo "write through /proc/<pid>/root: $(result bash -c "echo written >> '/proc/$pid/root$marker'")"
echo "read through /proc/<pid>/cwd: $(result cat "/proc/$pid/cwd/marker")"
echo "read through /proc/<pid>/fd: $(result cat "/proc/$pid/fd/3")"
EOF
chmod +x "$DIR/data/user_scripts/probe.sh"

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <listen_host>127.0.0.1</listen_host>
    <tcp_port>0</tcp_port>
    <path>$DIR/data/</path>
    <user_scripts_path>$DIR/data/user_scripts/</user_scripts_path>
    <filesystem_isolation>enforce</filesystem_isolation>
    <seccomp>trap</seccomp>
    <!-- For the shell of the probe. The directory of the marker is not visible. -->
    <filesystem_isolation_paths>
        <read_only>/bin</read_only>
        <read_only>/lib</read_only>
        <read_only>/lib64</read_only>
        <read_only>/usr</read_only>
    </filesystem_isolation_paths>
    <users><default><password></password><profile>default</profile><quota>default</quota><networks><ip>127.0.0.1</ip></networks></default></users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF

PID=
OUTSIDE=
function finish()
{
    [ -n "$PID" ] && kill "$PID" 2>/dev/null
    [ -n "$OUTSIDE" ] && kill "$OUTSIDE" 2>/dev/null
    wait
    mountpoint -q "$DIR/data/host_proc" && umount "$DIR/data/host_proc"
}
trap finish EXIT

# The same user as the server, no capabilities at all, dumpable, in the directory of the marker, with the marker open.
(cd "$DIR/outside" && exec setpriv --bounding-set=-all --inh-caps=-all sleep 600 3<"$MARKER") &
OUTSIDE=$!

$CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
PID=$!

PORT=
for _ in {1..600}; do
    PORT=$(grep -oE 'Listening for native protocol \(tcp\): 127\.0\.0\.1:[0-9]+' "$DIR/log/server.log" 2>/dev/null | head -n1 | grep -oE '[0-9]+$')
    [ -n "$PORT" ] && grep -q 'Ready for connections' "$DIR/log/server.log" && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done

if [ -z "$PORT" ] || ! grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null; then
    echo "The server did not start"
    cat "$DIR/server.out" "$DIR/log/server.err.log" >&2
    exit 1
fi

echo "outside process without capabilities: $(grep -qE '^CapEff:[[:space:]]+0+$' "/proc/$OUTSIDE/status" && grep -qE '^CapPrm:[[:space:]]+0+$' "/proc/$OUTSIDE/status" && echo yes || echo no)"
echo "namespaces: $(grep -oE 'Isolated the filesystem of this process in new [a-zA-Z, ]+ namespaces' "$DIR/log/server.log" | sed -E 's/.* in new (.*) namespaces/\1/')"
echo "seccomp filter of the server: $(grep -q 'Applied a seccomp policy' "$DIR/log/server.log" && echo installed || echo absent)"

$CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$PORT" --query "
    SELECT * FROM executable('probe.sh', TabSeparatedRaw, 'line String', (SELECT '$OUTSIDE', '$MARKER'))"

echo "marker: $([ "$(cat "$MARKER")" = "outside" ] && echo unchanged || echo "changed: $(cat "$MARKER")")"

# A `proc` of the PID namespace of the host inside `path`, which the server would see under the bind mount of `path`.
kill "$PID"
wait "$PID"
PID=
mkdir -p "$DIR/data/host_proc"
mount -t proc proc "$DIR/data/host_proc"
rm -f "$DIR/log/server.log"
$CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
PID=$!
for _ in {1..600}; do
    grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done
if grep -q 'Cannot isolate the filesystem: .* has a mount of `proc`' "$DIR/server.out" && ! kill -0 "$PID" 2>/dev/null; then
    echo "proc of the host inside path: refused"
else
    echo "proc of the host inside path: $(kill -0 "$PID" 2>/dev/null && echo started || echo failed)"
    tail -n 3 "$DIR/server.out"
fi
umount "$DIR/data/host_proc"
