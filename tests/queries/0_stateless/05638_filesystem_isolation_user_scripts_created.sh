#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` and a `user_scripts_path` that does not exist yet, as in a new data directory,
# creates it before the isolation, so that it is a read-only mount of its own where programs can be executed, also the
# ones added after the startup, rather than a part of the `noexec` mount of `path`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="directory of the scripts: created
mount of the scripts: read-only, executable"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_user_scripts_created"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/log"
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
    [ -n "$PID" ] && kill "$PID" 2>/dev/null
    wait
}
trap finish EXIT

$CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
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

echo "directory of the scripts: $([ -d "$DIR/data/user_scripts" ] && echo created || echo absent)"
# The options of the mount at `user_scripts` inside the server: `ro` without `noexec`.
OPTIONS=$(awk -v dir="$DIR/data/user_scripts" '$2 == dir { print $4 }' "/proc/$PID/mounts" | tail -n1)
if [ -z "$OPTIONS" ]; then
    echo "mount of the scripts: absent"
elif [[ ",$OPTIONS," == *",ro,"* && ",$OPTIONS," != *",noexec,"* ]]; then
    echo "mount of the scripts: read-only, executable"
else
    echo "mount of the scripts: $OPTIONS"
fi
