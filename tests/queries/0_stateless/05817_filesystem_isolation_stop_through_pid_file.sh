#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# With the isolation, the pid file has the PID of the supervisor outside of it. With `CLICKHOUSE_WATCHDOG_NO_FORWARD=1`,
# as in the systemd unit, the supervisor does not forward the signals of the service manager, which signals every
# process of the service, but `clickhouse stop` through the pid file still stops the server gracefully. A signal to the
# process group of the supervisor, as `kill -TERM -<pgid>`, reaches the server once, and stops it gracefully as well,
# rather than as a second termination signal, which terminates the server immediately.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="pid file: supervisor
stop: done
server: stopped gracefully
pid file: removed
process group: stopped gracefully"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
# The supervisor and the new PID namespace are used with `CAP_SYS_ADMIN` (bit 21), as the server started here has it.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ] \
    || ! (( (16#$(awk '$1 == "CapEff:" { print $2 }' /proc/self/status) >> 21) & 1 )); then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_stop_through_pid_file"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/log" "$DIR/run"
DIR=$(cd "$DIR" && pwd -P)

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>$DIR/data/</path>
    <!-- Any free port: nothing connects to this server. -->
    <tcp_port>0</tcp_port>
    <filesystem_isolation>enforce</filesystem_isolation>
    <users><default><password></password><profile>default</profile><quota>default</quota></default></users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF

PID=
function finish()
{
    [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null
    wait
}
trap finish EXIT

CLICKHOUSE_WATCHDOG_ENABLE=1 CLICKHOUSE_WATCHDOG_NO_FORWARD=1 \
    $CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml" --pid-file "$DIR/run/clickhouse-server.pid" > "$DIR/server.out" 2>&1 &
PID=$!

for _ in {1..600}; do
    grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done

if ! grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null || ! grep -q 'Isolated the filesystem' "$DIR/log/server.log"; then
    echo "The server did not start with an isolated filesystem"
    cat "$DIR/server.out" "$DIR/log/server.err.log" >&2
    exit 1
fi

echo "pid file: $([ "$(cat "$DIR/run/clickhouse-server.pid")" = "$PID" ] && echo supervisor || echo other)"

# `--do-not-kill`: only `SIGTERM`, never `SIGKILL`.
if $CLICKHOUSE_BINARY stop --prefix / --pid-path "$DIR/run" --max-tries 60 --do-not-kill > "$DIR/stop.out" 2>&1; then
    echo "stop: done"
else
    echo "stop: failed"
    tail -n 3 "$DIR/stop.out"
    kill -9 "$PID"
fi

wait "$PID"
STATUS=$?
PID=
echo "server: $([ "$STATUS" = 0 ] && grep -q 'Received termination signal' "$DIR/log/server.log" && echo "stopped gracefully" || echo "exit status $STATUS")"
echo "pid file: $([ -e "$DIR/run/clickhouse-server.pid" ] && echo present || echo removed)"

# Without the watchdog, in a process group of its own, which the signal goes to.
rm -f "$DIR/log/server.log"
CLICKHOUSE_WATCHDOG_ENABLE=0 setsid $CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
PID=$!
for _ in {1..600}; do
    grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done
kill -TERM -- "-$PID"
wait "$PID"
STATUS=$?
PID=
echo "process group: $([ "$STATUS" = 0 ] && grep -q 'Received termination signal' "$DIR/log/server.log" \
    && ! grep -q 'Received second termination signal' "$DIR/log/server.log" && echo "stopped gracefully" || echo "exit status $STATUS")"
