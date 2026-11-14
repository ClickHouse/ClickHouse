#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` and a directory of CA certificates in `openSSL.client.caConfig` whose hashed
# links, as `c_rehash` makes them, point to certificates in another directory, outside every other mount, keeps that
# directory visible, so that the links are not dangling inside, also for a chain of links through another directory, and
# for a link through a directory that is a link back to the directory of the certificates.
# The client context of OpenSSL is created only when it is used, so a placeholder for the certificate is enough.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="certificates read inside through the links: 3"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_ca_directory_links"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/log" "$DIR/ca" "$DIR/store/certificates" "$DIR/links" "$DIR/other_store" "$DIR/via_parent" "$DIR/out"
DIR=$(cd "$DIR" && pwd -P)
echo "placeholder" > "$DIR/store/certificates/private_ca.pem"
ln -s ../store/certificates/private_ca.pem "$DIR/ca/0123abcd.0"
echo "placeholder" > "$DIR/other_store/second_ca.pem"
ln -s "$DIR/other_store/second_ca.pem" "$DIR/links/second_ca.pem"
ln -s "$DIR/links/second_ca.pem" "$DIR/ca/89abcdef.0"
echo "placeholder" > "$DIR/ca/inner_ca.pem"
ln -s "$DIR/ca" "$DIR/via_parent/via"
ln -s "$DIR/via_parent/via/inner_ca.pem" "$DIR/ca/fedcba98.0"
# The server reads the certificates with `file` from its `user_files_path`, through absolute links that are followed
# inside its isolated filesystem.
for name in 0123abcd.0 89abcdef.0 fedcba98.0; do
    ln -s "$DIR/ca/$name" "$DIR/out/$name"
done

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>$DIR/data/</path>
    <openSSL><client><caConfig>$DIR/ca/</caConfig></client></openSSL>
    <user_files_path>$DIR/out/</user_files_path>
    <startup_scripts>
        <scripts>
            <query>INSERT INTO FUNCTION file('result.tsv', 'TSV', 'c UInt64') SELECT count() FROM file('{0123abcd.0,89abcdef.0,fedcba98.0}', 'LineAsString')</query>
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

RESULT="$DIR/out/result.tsv"
# The lookup is done inside the isolated server, by a startup script that reads the certificates through the links
# with `file` and writes how many lines it read, because an absolute link seen from outside, through `/proc/<pid>/root`,
# leads to the root of the observer instead. The startup script can finish after the server is ready.
for _ in {1..600}; do
    [ -s "$RESULT" ] && break
    grep -q 'Failed to parse startup scripts' "$DIR/log/server.err.log" 2>/dev/null && break
    sleep 0.1
done
echo "certificates read inside through the links: $(cat "$RESULT" 2>/dev/null || grep -m1 -o 'Code: [0-9]*.*' "$DIR/log/server.err.log")"
