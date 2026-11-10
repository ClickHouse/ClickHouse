#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` and a log in `/dev`, such as `logger.stderr` set to `/dev/null`, keeps its own
# private `/dev`: the devices of the host and its pseudoterminals stay invisible.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="dev: fd full null ptmx pts random stderr stdin stdout urandom zero
dev/pts: ptmx"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_dev_log"
rm -rf "$DIR"
mkdir -p "$DIR/data" "$DIR/log"

cat > "$DIR/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
        <stderr>/dev/null</stderr>
        <stdout>/dev/stdout</stdout>
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

$CLICKHOUSE_BINARY server --config-file "$DIR/config.xml" > "$DIR/server.out" 2>&1 &
PID=$!

function finish()
{
    kill "$PID" 2>/dev/null
    wait
}
trap finish EXIT

for _ in {1..600}; do
    grep -q 'Isolated the filesystem' "$DIR/log/server.log" 2>/dev/null && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done

if ! grep -q 'Isolated the filesystem' "$DIR/log/server.log" 2>/dev/null; then
    echo "The server did not isolate its filesystem"
    cat "$DIR/server.out" "$DIR/log/server.err.log" >&2
    exit 1
fi

# What the server sees in `/dev`, from outside through its root.
echo "dev: $(ls "/proc/$PID/root/dev" | paste -sd ' ')"
echo "dev/pts: $(ls "/proc/$PID/root/dev/pts" | paste -sd ' ')"
