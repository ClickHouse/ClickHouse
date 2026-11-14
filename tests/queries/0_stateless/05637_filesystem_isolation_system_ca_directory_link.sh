#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` keeps the CA certificates of the system readable when `/etc/ssl/certs` is a link
# to a directory of hashed links elsewhere, which point to certificates in yet another directory: the directory reached
# through the link is scanned for links as well. `/etc/ssl` is replaced in a private mount namespace, which disappears
# with the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="certificate of the system through the links: visible"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_system_ca_directory_link"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/log" "$DIR/ssl" "$DIR/hashed" "$DIR/private"
DIR=$(cd "$DIR" && pwd -P)
echo "placeholder" > "$DIR/private/root.pem"
ln -s "$DIR/private/root.pem" "$DIR/hashed/0123abcd.0"
ln -s "$DIR/hashed" "$DIR/ssl/certs"

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

cat > "$DIR/start.sh" <<EOF
set -e
mount --make-rprivate /
mkdir -p /etc/ssl
mount --bind "$DIR/ssl" /etc/ssl
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

# `unshare` executes the server in place of itself, so the process is the server. Through the links, as OpenSSL reads
# the certificate inside.
echo "certificate of the system through the links: $([ -f "/proc/$PID/root/etc/ssl/certs/0123abcd.0" ] && echo visible || echo invisible)"
