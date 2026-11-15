#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` and a file in its configuration that is a link whose target has `..` after
# another link, as in `server.log -> versions/current/../server.log` with `versions/current -> real/active`, finds
# the file where the kernel does: `real/server.log`, the parent of the target of `versions/current`, rather than
# `versions/server.log`. The certificates go through the same collection of paths as the logs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="log in the parent of the target: written
target of the link: visible"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_link_target_parent"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/log/versions" "$DIR/real/active"
ln -s "$DIR/real/active" "$DIR/log/versions/current"
ln -s "versions/current/../server.log" "$DIR/log/server.log"

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/data/server.err.log</errorlog>
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

$CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
PID=$!

for _ in {1..600}; do
    grep -q 'Ready for connections' "$DIR/real/server.log" 2>/dev/null && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done

if ! grep -q 'Ready for connections' "$DIR/real/server.log" 2>/dev/null || ! grep -q 'Isolated the filesystem' "$DIR/real/server.log"; then
    echo "The server did not start with an isolated filesystem"
    cat "$DIR/server.out" "$DIR/data/server.err.log" >&2
    exit 1
fi

echo "log in the parent of the target: $([ -s "$DIR/real/server.log" ] && echo written || echo missing)"
echo "target of the link: $([ -d "/proc/$PID/root$DIR/real/active" ] && echo visible || echo invisible)"
