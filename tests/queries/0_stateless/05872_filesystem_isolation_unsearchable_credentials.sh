#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` starts when `AWS_SHARED_CREDENTIALS_FILE` names a file in a directory that it
# cannot search, as `/root/.aws/credentials` for a server that does not run as root: it cannot read that file anyway.
# The same for `~/.aws/credentials` that is a link to a file in such a directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="started with an isolated filesystem: yes"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ] \
    || ! command -v setpriv >/dev/null; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_unsearchable_credentials"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/home/.aws" "$DIR/private/.aws" "$DIR/secrets/mounted"
ln -s "$DIR/secrets/mounted/credentials" "$DIR/home/.aws/credentials"
chmod 0 "$DIR/private" "$DIR/secrets"

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/data/server.log</log>
        <errorlog>$DIR/data/server.err.log</errorlog>
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

PID=
function finish()
{
    [ -n "$PID" ] && kill "$PID" 2>/dev/null
    wait
    chmod 0755 "$DIR/private" "$DIR/secrets"
}
trap finish EXIT

# Without the capabilities that bypass the permissions of files, root cannot search `private` and `secrets`, as a
# server that does not run as root cannot search `/root`.
env -u AWS_CONFIG_FILE AWS_SHARED_CREDENTIALS_FILE="$DIR/private/.aws/credentials" HOME="$DIR/home" \
    setpriv --bounding-set=-dac_override,-dac_read_search --inh-caps=-dac_override,-dac_read_search \
    $CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
PID=$!

for _ in {1..600}; do
    grep -q 'Ready for connections' "$DIR/data/server.log" 2>/dev/null && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done

if grep -q 'Ready for connections' "$DIR/data/server.log" 2>/dev/null && grep -q 'Isolated the filesystem' "$DIR/data/server.log"; then
    echo "started with an isolated filesystem: yes"
else
    echo "started with an isolated filesystem: no"
    cat "$DIR/server.out" "$DIR/data/server.err.log" >&2
fi
