#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` and whitespace around its `path`, which the server trims, as in a value on its
# own lines, uses the same directory as without the isolation, and finds its existing data there after a restart.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="start 1: data: existing metadata
start 2: data: existing metadata"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_path_whitespace"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/log" "$DIR/work" "$DIR/data"
DIR=$(cd "$DIR" && pwd -P)
touch "$DIR/data/existing"

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>
        $DIR/data/
    </path>
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

    echo "start $start: data: $(ls "/proc/$PID/root$DIR/data" | grep -E '^(existing|metadata)$' | paste -sd ' ')"

    kill "$PID"
    wait "$PID"
    PID=
done
