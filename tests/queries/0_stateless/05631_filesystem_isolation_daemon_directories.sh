#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# With `--daemon`, `BaseDaemon::initialize` changes to the directory of the executable, then to the directory of the
# log, and at its end to the directory of the core dumps, and each relative path is resolved where it is used. A server
# with `filesystem_isolation` keeps visible the paths it really uses: here `logger.log` relative to the directory of the
# executable, `core_path` relative to the directory of the log, and `named_collections_storage.path` relative to the
# directory of the core dumps, each with `..`, so that every one of them is outside the others.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="log: written
directory of the core dumps: visible
named collections: visible"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_daemon_directories"
rm -rf "$DIR"
mkdir -p "$DIR/bin" "$DIR/conf" "$DIR/data" "$DIR/launch"
# Absolute and without links, as the paths seen through `/proc/<pid>/root`.
DIR=$(cd "$DIR" && pwd -P)
# The daemon changes to the directory of the executable as it is named in `argv[0]`, without resolving the link.
ln -s "$(command -v "$CLICKHOUSE_BINARY")" "$DIR/bin/clickhouse-server"

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>../log/server.log</log>
        <errorlog>../log/server.err.log</errorlog>
    </logger>
    <path>$DIR/data/</path>
    <core_path>../runtime/cores</core_path>
    <named_collections_storage><path>../collections</path></named_collections_storage>
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
    [ -n "$PID" ] && kill "$PID" 2>/dev/null
    rm -f "$DIR/bin/clickhouse-server"
}
trap finish EXIT

# Without the watchdog, the process in the pid file is the isolated one.
(cd "$DIR/launch" && CLICKHOUSE_WATCHDOG_ENABLE=0 exec "$DIR/bin/clickhouse-server" --daemon \
    --config-file "$DIR/conf/config.xml" --pid-file "$DIR/data/server.pid") > "$DIR/server.out" 2>&1

for _ in {1..600}; do
    grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null && break
    sleep 0.1
done
PID=$(cat "$DIR/data/server.pid" 2>/dev/null)

if ! grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null || ! grep -q 'Isolated the filesystem' "$DIR/log/server.log"; then
    echo "The server did not start with an isolated filesystem"
    cat "$DIR/server.out" "$DIR/log/server.err.log" "$DIR/log/stderr.log" >&2
    exit 1
fi

echo "log: $([ -s "$DIR/log/server.log" ] && echo written || echo missing)"
echo "directory of the core dumps: $([ -d "/proc/$PID/root$DIR/runtime/cores" ] && echo visible || echo invisible)"
echo "named collections: $([ -d "/proc/$PID/root$DIR/runtime/collections" ] && echo visible || echo invisible)"

kill "$PID"
for _ in {1..600}; do
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done
PID=
