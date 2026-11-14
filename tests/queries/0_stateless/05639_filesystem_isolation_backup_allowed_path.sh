#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` keeps writable the directory where the `File` backup engine really writes: a
# relative `backups.allowed_path` is joined to `path` and normalized lexically, so with `path` a link to another
# directory, `../backups` is next to the link, not next to its target. The backup is made by startup scripts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="backup next to the link: written
backup next to the target of the link: absent"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_backup_allowed_path"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/log" "$DIR/storage/data" "$DIR/backups"
DIR=$(cd "$DIR" && pwd -P)
ln -s storage/data "$DIR/data-link"

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>$DIR/data-link/</path>
    <backups><allowed_path>../backups</allowed_path></backups>
    <startup_scripts>
        <scripts>
            <query>CREATE TABLE IF NOT EXISTS default.t (x UInt8) ENGINE = MergeTree ORDER BY x</query>
        </scripts>
        <scripts>
            <query>BACKUP TABLE default.t TO File('b')</query>
        </scripts>
    </startup_scripts>
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

# The startup scripts can finish after the server is ready.
for _ in {1..600}; do
    [ -e "$DIR/backups/b/.backup" ] && break
    grep -q 'Failed to parse startup scripts' "$DIR/log/server.err.log" 2>/dev/null && break
    sleep 0.1
done

echo "backup next to the link: $([ -e "$DIR/backups/b/.backup" ] && echo written || grep -m1 -o 'Code: [0-9]*[^(]*' "$DIR/log/server.err.log")"
echo "backup next to the target of the link: $([ -e "$DIR/storage/backups/b" ] && echo written || echo absent)"
