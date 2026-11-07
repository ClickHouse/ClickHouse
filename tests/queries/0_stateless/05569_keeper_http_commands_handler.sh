#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: starts `clickhouse keeper`, an entrypoint the fasttest build does not have.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The keeper HTTP control API (docs/en/guides/sre/keeper/index.md, "HTTP control") answers keeper
# client commands and four-letter words under /api/v1/commands, and is built only when
# `keeper_server.http_control.port` is set - which no stateless server config does, hence the keeper
# of this test, started as in 04936_keeper_handler_config_error.sh.
if ! "$CLICKHOUSE_BINARY" help 2>&1 | grep -qF 'clickhouse keeper [args]'; then
    exit 0
fi

TMP_DIR="$(mktemp -d "${CLICKHOUSE_TMP}/keeper_http_commands.XXXXXX")"
KEEPER_PID=""
NODE="n_${CLICKHOUSE_DATABASE}"

# The storage and log directories must outlive their process, so a keeper is waited for first.
function cleanup()
{
    if [ -n "$KEEPER_PID" ] && kill -0 "$KEEPER_PID" 2>/dev/null; then
        kill "$KEEPER_PID" 2>/dev/null
        wait "$KEEPER_PID" 2>/dev/null
    fi
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# Three free ports are needed, and keeper refuses a raft port equal to its own tcp_port, so none can
# be left at 0. They are random and retried on collision rather than probed: nothing here is unique
# per concurrent copy, so a derived port would be the same in every copy, and probe-then-bind is a
# race those copies lose. The window keeps out of the ephemeral range, which TIME_WAIT fills up.
read -r EPH_LOW EPH_HIGH <<< "$(cat /proc/sys/net/ipv4/ip_local_port_range 2>/dev/null)"
: "${EPH_LOW:=32768}" "${EPH_HIGH:=60999}"
if [ "$((EPH_LOW - 1024))" -ge "$((65535 - EPH_HIGH))" ]; then
    WIN_LOW=1024; WIN_HIGH=$((EPH_LOW - 1))
else
    WIN_LOW=$((EPH_HIGH + 1)); WIN_HIGH=65535
fi

# A slot holds all three ports; a range covering everything leaves one, so fall back to every port.
WIN_SLOTS=$(((WIN_HIGH - 2 - WIN_LOW) / 4 + 1))
if [ "$WIN_SLOTS" -lt 256 ]; then
    WIN_LOW=1024; WIN_HIGH=65535; WIN_SLOTS=$(((WIN_HIGH - 2 - WIN_LOW) / 4 + 1))
fi

# Readiness is the control listener answering, waited for by curl's own retry, which covers both a
# refused connection and the 503 of a keeper without a leader yet. A keeper that is gone is retried.
READY=000
for _ in $(seq 1 10); do
    PORT_BASE=$((WIN_LOW + RANDOM % WIN_SLOTS * 4))
    rm -rf "${TMP_DIR}/coordination" "${TMP_DIR}/keeper.log" "${TMP_DIR}/keeper.err.log"

    cat > "${TMP_DIR}/keeper_config.xml" <<EOF
<clickhouse><listen_host>127.0.0.1</listen_host>
    <logger><level>information</level><console>0</console>
        <log>${TMP_DIR}/keeper.log</log><errorlog>${TMP_DIR}/keeper.err.log</errorlog></logger>
    <keeper_server>
        <server_id>1</server_id><tcp_port>$((PORT_BASE + 1))</tcp_port>
        <http_control><port>${PORT_BASE}</port></http_control>
        <log_storage_path>${TMP_DIR}/coordination/log</log_storage_path>
        <snapshot_storage_path>${TMP_DIR}/coordination/snapshots</snapshot_storage_path>
        <coordination_settings><force_sync>false</force_sync>
            <raft_logs_level>warning</raft_logs_level><heart_beat_interval_ms>100</heart_beat_interval_ms>
            <election_timeout_lower_bound_ms>200</election_timeout_lower_bound_ms>
            <election_timeout_upper_bound_ms>400</election_timeout_upper_bound_ms></coordination_settings>
        <raft_configuration><server><id>1</id><hostname>127.0.0.1</hostname>
            <port>$((PORT_BASE + 2))</port></server></raft_configuration>
    </keeper_server>
</clickhouse>
EOF

    "$CLICKHOUSE_BINARY" keeper --config="${TMP_DIR}/keeper_config.xml" > "${TMP_DIR}/keeper.out" 2>&1 &
    KEEPER_PID=$!

    URL="http://127.0.0.1:${PORT_BASE}"
    READY="$(${CLICKHOUSE_CURL} -o /dev/null -w '%{http_code}' --retry 6 --retry-connrefused \
                 --retry-delay 0 --retry-max-time 120 --max-time 10 "${URL}/ready")"
    [ "$READY" = 200 ] && break

    kill "$KEEPER_PID" 2>/dev/null
    wait "$KEEPER_PID" 2>/dev/null
    KEEPER_PID=""
done

if [ "$READY" != 200 ]; then
    echo "keeper did not become ready: ${READY}" >&2
    cat "${TMP_DIR}/keeper.out" "${TMP_DIR}/keeper.err.log" >&2 2>/dev/null
    exit 1
fi

# $1 = label, $2 = target. Status only, where the body is a fixed sentence or an exception message.
function probe_status()
{
    printf '%s\t%s\n' "$1" "$(${CLICKHOUSE_CURL} -o /dev/null -w '%{http_code}' --max-time 10 "${URL}$2")"
}
# The same, plus the JSON document the handler builds for a command that ran.
function probe_body()
{
    local code
    code="$(${CLICKHOUSE_CURL} -o "${TMP_DIR}/body" -w '%{http_code}' --max-time 10 "${URL}$2")"
    printf '%s\t%s\t%s\n' "$1" "$code" "$(cat "${TMP_DIR}/body")"
}

# A percent escape the request path cannot be decoded with, and a command that is there but empty.
probe_status 'undecodable path' '/api/v1/commands/%zz'
probe_status 'empty command' '/api/v1/commands?command='

# `cwd` is what a relative path resolves against: the same `get .` reads the root, then the node.
probe_body 'create' "/api/v1/commands?command=create%20%2F${NODE}%20hello"
probe_body 'get at the default cwd' '/api/v1/commands?command=get%20.'
probe_body 'get at the given cwd' "/api/v1/commands?cwd=%2F${NODE}&command=get%20."

# A four-letter word the keeper knows runs in the keeper; one it does not know goes to the client.
probe_body 'ruok' '/api/v1/commands?command=ruok'
probe_status 'unknown four letter word' '/api/v1/commands?command=flwc%20ruok'
