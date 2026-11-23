#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` keeps writable the directory where the `File` backup engine really writes: a
# relative `backups.allowed_path` is joined to `path` as the server uses it and normalized lexically. An absolute `path`
# is used as it is written, so with `path` a link to another directory, `../backups` is next to the link; a relative
# `path` has its links resolved, so `../backups` is next to the target of the link. The backup is made by startup
# scripts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="absolute path: backup written where expected
relative path: backup written where expected"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_backup_allowed_path"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/log"
DIR=$(cd "$DIR" && pwd -P)

PID=
function finish()
{
    [ -n "$PID" ] && kill "$PID" 2>/dev/null
    wait
}
trap finish EXIT

# The case, the directory the server starts in, the value of `path`, where the backup is expected, and where not.
function run()
{
    local name=$1 work=$2 path=$3 expected=$4 unexpected=$5
    cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>$path</path>
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

    rm -f "$DIR/log/server.log" "$DIR/log/server.err.log"
    (cd "$work" && exec $CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml") > "$DIR/server.out" 2>&1 &
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
        [ -e "$expected/b/.backup" ] && break
        grep -q 'Failed to parse startup scripts' "$DIR/log/server.err.log" 2>/dev/null && break
        sleep 0.1
    done

    if [ -e "$expected/b/.backup" ] && [ ! -e "$unexpected/b" ]; then
        echo "$name: backup written where expected"
    else
        echo "$name: $(grep -m1 -o 'Code: [0-9]*[^(]*' "$DIR/log/server.err.log" || echo 'backup elsewhere')"
    fi

    kill "$PID"
    wait "$PID"
    PID=
}

# An absolute `path` that is a link: the backups are next to the link.
mkdir -p "$DIR/absolute/storage/data" "$DIR/absolute/backups"
ln -s storage/data "$DIR/absolute/data-link"
run "absolute path" "$DIR" "$DIR/absolute/data-link/" "$DIR/absolute/backups" "$DIR/absolute/storage/backups"

# A relative `path` that is a link: the backups are next to the target of the link.
mkdir -p "$DIR/relative/work" "$DIR/relative/storage/data" "$DIR/relative/storage/backups"
ln -s "$DIR/relative/storage/data" "$DIR/relative/work/data-link"
run "relative path" "$DIR/relative/work" "data-link/" "$DIR/relative/storage/backups" "$DIR/relative/work/backups"
