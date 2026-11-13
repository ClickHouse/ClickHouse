#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` keeps visible the dynamic loader and the shared libraries of
# `clickhouse-odbc-bridge` next to its executable, which it starts after the isolation, also the ones that the server
# itself does not map. A dynamically linked program of the system stands for the bridge here.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="libraries of the bridge: visible"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_bridge_libraries"
rm -rf "$DIR"
mkdir -p "$DIR/bin" "$DIR/conf" "$DIR/data" "$DIR/log"
DIR=$(cd "$DIR" && pwd -P)
# The server looks for the bridge in the directory of its executable as it is named in `argv[0]`.
ln -s "$(command -v "$CLICKHOUSE_BINARY")" "$DIR/bin/clickhouse-server"
cp "$(command -v ls)" "$DIR/bin/clickhouse-odbc-bridge"

# The loader and the libraries of the stand-in, without their links.
LIBRARIES=$(ldd "$DIR/bin/clickhouse-odbc-bridge" | grep -o -E '/[^ ]+' | xargs -n1 readlink -f | sort -u)
if [ -z "$LIBRARIES" ]; then
    echo "The stand-in for the bridge is not dynamically linked" >&2
    exit 1
fi

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
    [ -n "$PID" ] && kill "$PID" 2>/dev/null
    wait
}
trap finish EXIT

"$DIR/bin/clickhouse-server" --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
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

MISSING=
for library in $LIBRARIES; do
    [ -e "/proc/$PID/root$library" ] || MISSING="$MISSING $library"
done
if [ -z "$MISSING" ]; then
    echo "libraries of the bridge: visible"
else
    echo "libraries of the bridge: missing$MISSING"
fi
