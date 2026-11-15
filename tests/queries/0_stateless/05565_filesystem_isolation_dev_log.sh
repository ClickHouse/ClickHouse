#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` and a log in `/dev`, such as `logger.stdout` set to `/dev/stdout`, or a log file
# that is a link to `/dev/null`, keeps its own private `/dev`: the devices of the host and its pseudoterminals stay
# invisible. It also starts from a working directory outside of its other paths with an empty `access_control_path`,
# which does not make the working directory visible, and it finds the definitions of dictionaries in the working
# directory for a relative pattern that matches nothing next to the configuration file, as the loader does. The
# directory of a filesystem cache that a cache disk takes from a named collection is visible too.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="dev: fd full null ptmx pts random stderr stdin stdout urandom zero
dev/pts: ptmx
working directory: definitions
definitions: dictionary.xml
cache from a named collection: visible"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_dev_log"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/log" "$DIR/work/definitions"
ln -s /dev/null "$DIR/log/stderr.log"
# Must stay invisible: nothing else in the working directory is needed.
touch "$DIR/work/secret"
echo "<clickhouse></clickhouse>" > "$DIR/work/definitions/dictionary.xml"

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
        <stderr>$DIR/log/stderr.log</stderr>
        <stdout>/dev/stdout</stdout>
    </logger>
    <path>$DIR/data/</path>
    <access_control_path></access_control_path>
    <dictionaries_config>definitions/*.xml</dictionaries_config>
    <named_collections>
        <shared_cache>
            <path>$DIR/cache_from_named_collection/</path>
            <max_size>1073741824</max_size>
        </shared_cache>
    </named_collections>
    <storage_configuration>
        <disks>
            <local_disk>
                <type>object_storage</type>
                <object_storage_type>local_blob_storage</object_storage_type>
                <path>$DIR/disk/</path>
            </local_disk>
            <cache_disk>
                <type>cache</type>
                <disk>local_disk</disk>
                <cache_name>shared_cache</cache_name>
            </cache_disk>
        </disks>
    </storage_configuration>
    <!-- Any free port: nothing connects to this server. -->
    <tcp_port>0</tcp_port>
    <filesystem_isolation>enforce</filesystem_isolation>
    <users><default><password></password><profile>default</profile><quota>default</quota></default></users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF

(cd "$DIR/work" && exec $CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml") > "$DIR/server.out" 2>&1 &
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

# What the server sees, from outside through its root.
echo "dev: $(ls "/proc/$PID/root/dev" | paste -sd ' ')"
echo "dev/pts: $(ls "/proc/$PID/root/dev/pts" | paste -sd ' ')"
echo "working directory: $(ls "/proc/$PID/root$DIR/work" | paste -sd ' ')"
echo "definitions: $(ls "/proc/$PID/root$DIR/work/definitions" | paste -sd ' ')"
echo "cache from a named collection: $([ -d "/proc/$PID/root$DIR/cache_from_named_collection" ] && echo visible || echo invisible)"
# The checks above see the root of a running server only.
if ! kill -0 "$PID" 2>/dev/null; then
    echo "The server exited"
    cat "$DIR/server.out" "$DIR/log/server.err.log" >&2
fi
