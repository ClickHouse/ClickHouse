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

EXPECTED="certificates of the system read inside through the links: 1"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_system_ca_directory_link"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/log" "$DIR/ssl" "$DIR/hashed" "$DIR/private" "$DIR/out"
DIR=$(cd "$DIR" && pwd -P)
echo "placeholder" > "$DIR/private/root.pem"
ln -s "$DIR/private/root.pem" "$DIR/hashed/0123abcd.0"
ln -s "$DIR/hashed" "$DIR/ssl/certs"
# The server reads the certificate with `file` from its `user_files_path`, through an absolute link that is followed
# inside its isolated filesystem.
ln -s /etc/ssl/certs/0123abcd.0 "$DIR/out/0123abcd.0"

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>$DIR/data/</path>
    <user_files_path>$DIR/out/</user_files_path>
    <startup_scripts>
        <scripts>
            <query>INSERT INTO FUNCTION file('result.tsv', 'TSV', 'c UInt64') SELECT count() FROM file('0123abcd.0', 'LineAsString')</query>
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

RESULT="$DIR/out/result.tsv"
# The lookup is done inside the isolated server, by a startup script that reads the certificates through the links
# with `file` and writes how many lines it read, because an absolute link seen from outside, through `/proc/<pid>/root`,
# leads to the root of the observer instead. The startup script can finish after the server is ready.
for _ in {1..600}; do
    [ -s "$RESULT" ] && break
    grep -q 'Failed to parse startup scripts' "$DIR/log/server.err.log" 2>/dev/null && break
    sleep 0.1
done
echo "certificates of the system read inside through the links: $(cat "$RESULT" 2>/dev/null || grep -m1 -o 'Code: [0-9]*.*' "$DIR/log/server.err.log")"
