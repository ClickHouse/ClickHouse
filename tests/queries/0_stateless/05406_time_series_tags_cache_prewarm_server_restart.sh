#!/usr/bin/env bash
# A restarted server loads its tables on the asynchronous loader, outside of any query, and the tags
# deduplication cache of a `TimeSeries` table must still be filled from the tags table there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_prewarm_restart"
rm -rf "$work"
mkdir -p "$work"

pid=""
cleanup() {
    if [ -n "$pid" ]; then
        kill "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
    fi
    rm -rf "$work"
}
trap cleanup EXIT

cat > "$work/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$work/server.log</log>
        <errorlog>$work/server.err.log</errorlog>
        <console>0</console>
    </logger>
    <listen_host>127.0.0.1</listen_host>
    <tcp_port>0</tcp_port>
    <path>$work/data/</path>
    <users>
        <default>
            <password></password>
            <profile>default</profile>
            <quota>default</quota>
            <networks><ip>127.0.0.1</ip></networks>
        </default>
    </users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF

port=""

# Starts the server and sets `pid` and `port`. Every start writes a new log, so the port is read from the current one.
start_server() {
    rm -f "$work/server.log"
    $CLICKHOUSE_BINARY server --config-file="$work/config.xml" > "$work/stdout.log" 2>&1 &
    pid=$!

    port=""
    for _ in {1..1200}; do
        port=$(grep -oE 'Listening for native protocol \(tcp\): 127\.0\.0\.1:[0-9]+' "$work/server.log" 2>/dev/null | head -n1 | grep -oE '[0-9]+$')
        [ -n "$port" ] && break
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.1
    done

    if [ -z "$port" ]; then
        echo "The server did not start"
        grep -m1 -oE 'Code: [0-9]+\. DB::Exception: [^/]*' "$work/server.err.log" | sed 's/ *$//'
        exit 1
    fi

    for _ in {1..100}; do
        $CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$port" -q "SELECT 1" >/dev/null 2>&1 && break
        sleep 0.1
    done
}

stop_server() {
    kill "$pid"
    wait "$pid"
    pid=""
}

start_server
$CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$port" -q "
    SET allow_experimental_time_series_table = 1;
    CREATE TABLE ts_restart ENGINE = TimeSeries TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
    INSERT INTO ts_restart (metric_name, tags, samples) VALUES
        ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]),
        ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]);
"
stop_server

echo "--- a restarted server skips the time series already stored ---"
start_server
# The insert waits until the table has started, so the cache is already filled when the rows are hashed.
$CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$port" -q "
    INSERT INTO ts_restart (metric_name, tags, samples) VALUES
        ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:02', 3), 3.)]),
        ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:03', 3), 4.)]);
    SELECT count() FROM timeSeriesTags(default.ts_restart);
"
stop_server
