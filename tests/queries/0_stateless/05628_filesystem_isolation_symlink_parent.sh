#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` and a relative `path` with `..` after a link to a directory, as in
# `link/../data` with `link -> storage/active`, uses the same directory as without the isolation: `storage/data`, the
# parent of the target of the link, rather than `data` next to the link. Its existing data stays there after a restart.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="start 1: data: existing metadata
start 1: data next to the link: absent
start 1: target of the link: visible
start 2: data: existing metadata
start 2: data next to the link: absent
start 2: target of the link: visible"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_symlink_parent"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/log" "$DIR/work" "$DIR/storage/active" "$DIR/storage/data"
ln -s "$DIR/storage/active" "$DIR/work/link"
touch "$DIR/storage/data/existing"

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>link/../data/</path>
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

for start in 1 2; do
    rm -f "$DIR/log/server.log"
    (cd "$DIR/work" && exec $CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml") > "$DIR/server.out" 2>&1 &
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

    # What the server created, from outside, and what it sees, through its root.
    echo "start $start: data: $(ls "$DIR/storage/data" | grep -E '^(existing|metadata)$' | paste -sd ' ')"
    echo "start $start: data next to the link: $([ -e "$DIR/work/data" ] && echo present || echo absent)"
    echo "start $start: target of the link: $([ -d "/proc/$PID/root$DIR/storage/active" ] && echo visible || echo invisible)"

    kill "$PID"
    wait "$PID"
    PID=
done
