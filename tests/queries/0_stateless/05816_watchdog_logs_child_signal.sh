#!/usr/bin/env bash
# The watchdog writes to the log how the server process ended.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_watchdog_logs_child_signal"
rm -rf "$DIR"
mkdir -p "$DIR/data"

# The asynchronous logger is pinned, as the watchdog writes its last line right before it exits.
cat > "$DIR/config.xml" <<EOF
<clickhouse>
    <logger><level>information</level><log>$DIR/server.log</log><console>0</console><async>true</async></logger>
    <path>$DIR/data/</path>
    <tcp_port>0</tcp_port>
    <users><default><password></password><profile>default</profile><quota>default</quota></default></users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF

WATCHDOG=
CHILD=
function finish()
{
    [ -n "$CHILD" ] && kill -9 "$CHILD" 2>/dev/null
    [ -n "$WATCHDOG" ] && kill -9 "$WATCHDOG" 2>/dev/null && wait "$WATCHDOG" 2>/dev/null
    rm -rf "$DIR"
}
trap finish EXIT

CLICKHOUSE_WATCHDOG_ENABLE=1 $CLICKHOUSE_BINARY server --config-file "$DIR/config.xml" </dev/null >/dev/null 2>&1 &
WATCHDOG=$!

for _ in {1..600}; do
    CHILD=$(grep -oE 'Will watch for the process with pid [0-9]+' "$DIR/server.log" 2>/dev/null | grep -oE '[0-9]+$')
    [ -n "$CHILD" ] && break
    sleep 0.1
done

kill -9 "$CHILD"
wait "$WATCHDOG"
echo "watchdog exit code: $?"
WATCHDOG=
CHILD=

grep -c -F 'Child process was terminated by signal 9 (KILL)' "$DIR/server.log"
