#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` sees the token of the service account that Kubernetes projects into a pod for the
# IAM roles of EKS, named by `AWS_WEB_IDENTITY_TOKEN_FILE`, also after the kubelet rotates it: the token is a link
# `token -> ..data/token`, and the kubelet writes the new one into a new directory and points `..data` at it.
# A credential file directly in `/tmp` is visible on its own: the `/tmp` of the host does not cover the private
# writable one.
# The default directory of the AWS SDK, `~/.aws`, is visible also if `HOME` is not set: the SDK takes the home directory
# of the user from the passwd database then.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="token: first
config in /tmp: profile
unrelated file in /tmp: hidden
/tmp is writable: yes
rotated token: second
.aws in the home directory without HOME: visible"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_web_identity_token"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/serviceaccount/..2026_01_01_00_00_00.1"
echo first > "$DIR/serviceaccount/..2026_01_01_00_00_00.1/token"
ln -s "..2026_01_01_00_00_00.1" "$DIR/serviceaccount/..data"
ln -s "..data/token" "$DIR/serviceaccount/token"
TMP_CONFIG="/tmp/${CLICKHOUSE_DATABASE}_filesystem_isolation_aws_config"
TMP_UNRELATED="/tmp/${CLICKHOUSE_DATABASE}_filesystem_isolation_unrelated"
echo profile > "$TMP_CONFIG"
echo unrelated > "$TMP_UNRELATED"

# The home directory of the user in the passwd database. The check is skipped if there is none or it is not writable.
# Another instance of this test can use the directory at the same time, so it is removed only if it is empty.
PASSWD_HOME=$(getent passwd "$(id -u)" | cut -d: -f6)
AWS_MARKER=
if [[ "$PASSWD_HOME" == /* ]]; then
    for _ in {1..100}; do
        if mkdir -p "$PASSWD_HOME/.aws" 2>/dev/null && echo marker 2>/dev/null > "$PASSWD_HOME/.aws/${CLICKHOUSE_DATABASE}_marker"; then
            AWS_MARKER="$PASSWD_HOME/.aws/${CLICKHOUSE_DATABASE}_marker"
            break
        fi
        [ -w "$PASSWD_HOME" ] || break
    done
fi

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
    rm -f "$TMP_CONFIG" "$TMP_UNRELATED"
    if [ -n "$AWS_MARKER" ]; then
        rm -f "$AWS_MARKER"
        rmdir "$PASSWD_HOME/.aws" 2>/dev/null
    fi
}
trap finish EXIT

env -u HOME AWS_WEB_IDENTITY_TOKEN_FILE="$DIR/serviceaccount/token" AWS_CONFIG_FILE="$TMP_CONFIG" $CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
PID=$!

for _ in {1..600}; do
    grep -q 'Ready for connections' "$DIR/data/server.log" 2>/dev/null && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done

if ! grep -q 'Ready for connections' "$DIR/data/server.log" 2>/dev/null || ! grep -q 'Isolated the filesystem' "$DIR/data/server.log"; then
    echo "The server did not start with an isolated filesystem"
    cat "$DIR/server.out" "$DIR/data/server.err.log" >&2
    exit 1
fi
# With `CAP_SYS_ADMIN`, the supervisor stays outside of the namespaces, and its child, the init of the PID
# namespace, is inside. Otherwise that process is inside.
INSIDE=$PID
[ "$(cat "/proc/$PID/comm")" = clickhouse-isol ] && INSIDE=$(cut -d' ' -f1 "/proc/$PID/task/$PID/children")

echo "token: $(cat "/proc/$INSIDE/root$DIR/serviceaccount/token" 2>&1)"
echo "config in /tmp: $(cat "/proc/$INSIDE/root$TMP_CONFIG" 2>&1)"
echo "unrelated file in /tmp: $([ -e "/proc/$INSIDE/root$TMP_UNRELATED" ] && echo visible || echo hidden)"
echo "/tmp is writable: $(touch "/proc/$INSIDE/root/tmp/${CLICKHOUSE_DATABASE}_write" 2>/dev/null && echo yes || echo no)"

# As the kubelet does it (`AtomicWriter`): the new token in a new directory, a new link to it renamed over `..data`,
# and the old directory removed.
mkdir "$DIR/serviceaccount/..2026_01_02_00_00_00.2"
echo second > "$DIR/serviceaccount/..2026_01_02_00_00_00.2/token"
ln -s "..2026_01_02_00_00_00.2" "$DIR/serviceaccount/..data_tmp"
mv -T "$DIR/serviceaccount/..data_tmp" "$DIR/serviceaccount/..data"
rm -rf "$DIR/serviceaccount/..2026_01_01_00_00_00.1"

echo "rotated token: $(cat "/proc/$INSIDE/root$DIR/serviceaccount/token" 2>&1)"

if [ -z "$AWS_MARKER" ] || [ -e "/proc/$INSIDE/root$AWS_MARKER" ]; then
    echo ".aws in the home directory without HOME: visible"
else
    echo ".aws in the home directory without HOME: hidden"
fi
