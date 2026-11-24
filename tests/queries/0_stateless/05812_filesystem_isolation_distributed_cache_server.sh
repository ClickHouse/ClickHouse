#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` keeps writable the working directory of every
# `<distributed_cache_server>` section, which the instance creates at startup, and the cache inside it. Every section
# is covered, not only the first one; a relative `<cache><path>` that leaves the working directory is kept writable
# where it really is. The certificates of a section and the certificate chain of the client stay readable, and their
# directories are mounted, so that a renewed certificate read on a reload of the configuration, after the filesystem
# is isolated, is visible.
#
# The distributed cache is only in ClickHouse Cloud, but its configuration is respected by the isolation in every
# build, so the mounts are checked everywhere. Where the distributed cache is compiled in, the instances are really
# started and a renewed certificate is read after a reload; these checks print nothing when they pass.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="two sections: started
first section writable: 1
second section writable: 1
escaping cache writable: 1
server certificate directory visible: 1
server key directory visible: 1
client certificate chain directory visible: 1"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

HAS_DISTRIBUTED_CACHE=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'distributed_cache_server_settings'")

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_distributed_cache_server"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/log" "$DIR/run" "$DIR/outside" "$DIR/cert-only" "$DIR/key-only" "$DIR/chain-only"
DIR=$(cd "$DIR" && pwd -P)

PID=
function finish()
{
    [ -n "$PID" ] && kill "$PID" 2>/dev/null
    wait
}
trap finish EXIT

# Each file under a directory of its own, outside every path the server is given otherwise, so that only a mount of
# that one directory makes that one file readable: a directory holding two of them would be mounted for either.
openssl req -newkey rsa:2048 -nodes -keyout "$DIR/key-only/server.key" -x509 -days 1 \
    -out "$DIR/cert-only/server.crt" -subj "/CN=localhost" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$DIR/chain-only/chain.key" -x509 -days 1 \
    -out "$DIR/chain-only/chain.crt" -subj "/CN=localhost" >/dev/null 2>&1
rm -f "$DIR/chain-only/chain.key"

# The ports of the sections and the Raft port are left to the system. The port of Keeper goes into the `<zookeeper>`
# of every section and the port of the server is where the reload below is sent, so these two are bound, read and
# released, as in `05055_native_http_client_framing.sh`. Another process can take one of them between the probe and
# the server, especially when the test runs many times in parallel: then the server either does not start (`No
# servers started`) or Keeper does not listen. Such an attempt is stopped and repeated with new ports.
for _ in {1..10}; do
    read -r SERVER_PORT KEEPER_PORT <<<"$(python3 -c "
import socket
sockets = [socket.socket() for _ in range(2)]
for s in sockets:
    s.bind(('127.0.0.1', 0))
print(' '.join(str(s.getsockname()[1]) for s in sockets))
for s in sockets:
    s.close()
")"

    rm -rf "$DIR/log" "$DIR/run" "$DIR/first" "$DIR/second" "$DIR/outside/cache"
    mkdir -p "$DIR/log" "$DIR/run"

    # A Raft port equal to a port of the server is rejected, and 0 is the port of a server whose `<tcp_port>` is 0, so
    # the port of the server here is a concrete one.
    cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>$DIR/run</path>
    <tcp_port>$SERVER_PORT</tcp_port>
    <filesystem_isolation>enforce</filesystem_isolation>
    <keeper_server>
        <tcp_port>$KEEPER_PORT</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>$DIR/run/coordination/log</log_storage_path>
        <snapshot_storage_path>$DIR/run/coordination/snapshots</snapshot_storage_path>
        <raft_configuration><server><id>1</id><hostname>localhost</hostname><port>0</port></server></raft_configuration>
    </keeper_server>
    <zookeeper><node index="1"><host>localhost</host><port>$KEEPER_PORT</port></node></zookeeper>
    <distributed_cache_server>
        <local>1</local>
        <ci_mode>1</ci_mode>
        <tcp_port>0</tcp_port>
        <path>$DIR/first/</path>
        <tls_certificate_path>$DIR/cert-only/server.crt</tls_certificate_path>
        <tls_key_path>$DIR/key-only/server.key</tls_key_path>
        <cache><max_size>1Gi</max_size><path>cache/</path><cache_policy>slru_overcommit</cache_policy><check_cache_probability>0</check_cache_probability></cache>
        <zookeeper><node index="1"><host>localhost</host><port>$KEEPER_PORT</port></node></zookeeper>
    </distributed_cache_server>
    <distributed_cache_server>
        <local>1</local>
        <ci_mode>1</ci_mode>
        <tcp_port>0</tcp_port>
        <path>$DIR/second/</path>
        <!-- A relative cache path is joined to the working directory as it is written, so it can leave it. -->
        <cache><max_size>1Gi</max_size><path>../outside/cache/</path><cache_policy>lru_overcommit</cache_policy><check_cache_probability>0</check_cache_probability></cache>
        <zookeeper><node index="1"><host>localhost</host><port>$KEEPER_PORT</port></node></zookeeper>
    </distributed_cache_server>
    <!-- The registry of the clients is initialized after the filesystem is isolated, and reads the chain then. -->
    <distributed_cache_client>
        <local>1</local>
        <ci_mode>1</ci_mode>
        <client_id>test_isolation_client</client_id>
        <certificate_chain_path>$DIR/chain-only/chain.crt</certificate_chain_path>
        <zookeeper><node index="1"><host>localhost</host><port>$KEEPER_PORT</port></node></zookeeper>
    </distributed_cache_client>
    <users><default><password></password><profile>default</profile><quota>default</quota></default></users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF

    $CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
    PID=$!

    # Longer than the 60 seconds of the other tests of the isolation: every instance registers itself in Keeper and
    # initializes its cache before the server is ready.
    for _ in {1..1200}; do
        grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null && break
        grep -q 'Address already in use' "$DIR/log/server.log" 2>/dev/null && break
        kill -0 "$PID" 2>/dev/null || break
        sleep 0.1
    done

    grep -q 'Address already in use' "$DIR/log/server.log" 2>/dev/null || break
    kill "$PID" 2>/dev/null
    wait "$PID"
    PID=
done

if grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null && grep -q 'Isolated the filesystem' "$DIR/log/server.log"; then
    echo "two sections: started"
else
    echo "two sections: $(grep -m1 -oE 'Code: [0-9]+\..{0,160}' "$DIR/log/server.err.log" || echo 'did not start')"
    cat "$DIR/server.out" "$DIR/log/server.err.log" >&2
    exit 1
fi

# The server lists every mount as `<path> (<access>, <source>)`, with the path resolved, so the name followed by a
# space and a parenthesis is the directory itself and not a mount of a file inside it. The cache of the first section
# is inside its working directory and covered by its mount; the cache of the second one is a mount of its own.
VISIBLE=$(grep -m1 'Only these paths are visible' "$DIR/log/server.log")
function visible()
{
    case "$VISIBLE" in (*"$1 ($2"*) echo 1;; (*) echo 0;; esac
}
echo "first section writable: $(visible "$DIR/first" read-write)"
echo "second section writable: $(visible "$DIR/second" read-write)"
echo "escaping cache writable: $(visible "$DIR/outside/cache" read-write)"

# A location of a certificate is mounted as the whole directory, so that a renewed file replacing the current one is
# visible.
echo "server certificate directory visible: $(visible "$DIR/cert-only" read-only)"
echo "server key directory visible: $(visible "$DIR/key-only" read-only)"
echo "client certificate chain directory visible: $(visible "$DIR/chain-only" read-only)"

[ "$HAS_DISTRIBUTED_CACHE" = 1 ] || exit 0

# The instance of each section writes its uuid file into its own working directory: the second section proves every
# section is covered, not only the first.
[ -e "$DIR/second/distributed-cache-server-uuid" ] || echo "The second section did not write its uuid file" >&2

# Replacing the pair and reloading makes the server read it again, long after the isolation. The new pair is written
# beside the old one and renamed over it, as a renewal does, so that the mount of the directory is what makes the
# replacement visible. The key goes first, because the server rejects a certificate whose key is from another pair.
openssl req -newkey rsa:2048 -nodes -keyout "$DIR/key-only/server.key.new" -x509 -days 1 \
    -out "$DIR/cert-only/server.crt.new" -subj "/CN=localhost" >/dev/null 2>&1
mv "$DIR/key-only/server.key.new" "$DIR/key-only/server.key"
mv "$DIR/cert-only/server.crt.new" "$DIR/cert-only/server.crt"
$CLICKHOUSE_BINARY client --host 127.0.0.1 --port "$SERVER_PORT" --query "SYSTEM RELOAD CONFIG" > "$DIR/reload.out" 2>&1
RENEWED=0
for _ in {1..600}; do
    grep -q 'The published TLS certificate changed' "$DIR/log/server.log" && { RENEWED=1; break; }
    grep -q 'Could not reload the published distributed cache certificate' "$DIR/log/server.log" && break
    sleep 0.1
done
if [ "$RENEWED" != 1 ]; then
    echo "The renewed certificate was not read after the isolation" >&2
    cat "$DIR/reload.out" "$DIR/log/server.err.log" >&2
fi
