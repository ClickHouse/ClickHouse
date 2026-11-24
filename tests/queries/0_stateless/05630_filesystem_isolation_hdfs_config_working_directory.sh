#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` and a relative `hdfs.libhdfs3_conf` keeps visible the file that `Server::main`
# uses: the one in the working directory, and the one next to the main configuration only if there is none in the
# working directory. The keytabs of Kerberos for HDFS, also for a user of HDFS, and the directory of its cache of the
# tickets stay visible as well.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="in the working directory: visible
keytab: visible
keytab of a user: visible
cache of the tickets: writable"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_hdfs_config_working_directory"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/log" "$DIR/work/client" "$DIR/keys" "$DIR/user_keys" "$DIR/tickets"
touch "$DIR/keys/client.keytab" "$DIR/user_keys/alice.keytab"
echo '<configuration/>' > "$DIR/work/client/hdfs-site.xml"

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
    <hdfs>
        <libhdfs3_conf>client/hdfs-site.xml</libhdfs3_conf>
        <hadoop_kerberos_keytab>$DIR/keys/client.keytab</hadoop_kerberos_keytab>
        <hadoop_security_kerberos_ticket_cache_path>FILE:$DIR/tickets/krb5cc</hadoop_security_kerberos_ticket_cache_path>
    </hdfs>
    <hdfs_alice><hadoop_kerberos_keytab>$DIR/user_keys/alice.keytab</hadoop_kerberos_keytab></hdfs_alice>
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

echo "in the working directory: $([ -f "/proc/$PID/root$DIR/work/client/hdfs-site.xml" ] && echo visible || echo invisible)"
echo "keytab: $([ -f "/proc/$PID/root$DIR/keys/client.keytab" ] && echo visible || echo invisible)"
echo "keytab of a user: $([ -f "/proc/$PID/root$DIR/user_keys/alice.keytab" ] && echo visible || echo invisible)"
echo "cache of the tickets: $(awk -v dir="$DIR/tickets" '$2 == dir && $4 ~ /^rw(,|$)/' "/proc/$PID/mounts" | grep -q . && echo writable || echo absent)"
