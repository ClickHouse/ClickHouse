#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` starts when a path it mounts contains a mount covered by another one mounted on
# top of it, with a mount of its own inside: only the visible mounts are remounted, not the covered ones, which a lookup
# of their paths does not reach. The mounts are made in a private mount namespace, which disappears with the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="started with an isolated filesystem
visible mount on top: visible
covered mount inside: invisible"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_stacked_mounts"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/log" "$DIR/tree"
DIR=$(cd "$DIR" && pwd -P)

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>$DIR/data/</path>
    <user_files_path>$DIR/tree/</user_files_path>
    <!-- Any free port: nothing connects to this server. -->
    <tcp_port>0</tcp_port>
    <filesystem_isolation>enforce</filesystem_isolation>
    <users><default><password></password><profile>default</profile><quota>default</quota></default></users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF

# `stack` is a mount with `hidden` mounted inside it, and another mount on top of `stack` covers both.
cat > "$DIR/start.sh" <<EOF
set -e
mount --make-rprivate /
mount -t tmpfs tmpfs "$DIR/tree"
mkdir "$DIR/tree/stack"
mount -t tmpfs tmpfs "$DIR/tree/stack"
mkdir "$DIR/tree/stack/hidden"
mount -t tmpfs tmpfs "$DIR/tree/stack/hidden"
mount -t tmpfs tmpfs "$DIR/tree/stack"
touch "$DIR/tree/stack/on_top"
exec $CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml"
EOF

PID=
function finish()
{
    [ -n "$PID" ] && kill "$PID" 2>/dev/null
    wait
}
trap finish EXIT

unshare --mount bash "$DIR/start.sh" > "$DIR/server.out" 2>&1 &
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
echo "started with an isolated filesystem"

# `unshare` executes the server in place of itself, so the process is the server.
echo "visible mount on top: $([ -f "/proc/$PID/root$DIR/tree/stack/on_top" ] && echo visible || echo invisible)"
echo "covered mount inside: $([ -e "/proc/$PID/root$DIR/tree/stack/hidden" ] && echo visible || echo invisible)"
