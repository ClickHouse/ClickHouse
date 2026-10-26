#!/usr/bin/env bash
# Tags: no-fasttest, no-asan, no-tsan, no-msan
# Tag no-fasttest: starts the keeper entrypoint of the binary under test.
# Tag no-asan: jemalloc is not built in under asan, tsan or msan, so /jemalloc/status is a 501 there.
# Tag no-tsan: as no-asan.
# Tag no-msan: as no-asan.

# `GET /jemalloc/status` on Keeper's HTTP control port reports the live jemalloc profiling state as
# JSON - prof_enabled, prof_active, thread_active_init, lg_sample (and HEAD the status alone) - as
# documented in docs/concepts/features/performance/allocation-profiling.mdx. It is only installed
# when `keeper_server.http_control.port` is set, which the stateless server never is, and `opt.prof`
# is fixed when a process starts, so each arm starts a Keeper of its own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if ! "$CLICKHOUSE_BINARY" help 2>&1 | grep -qF 'clickhouse keeper [args]'; then
    exit 0
fi

TMP_DIR="$(mktemp -d "${CLICKHOUSE_TMP}/keeper_jemalloc_status.XXXXXX")"
CONSOLE="${TMP_DIR}/console"
KEEPER_PID=""

# The storage directories must outlive the process using them, so a Keeper left running is waited for.
function cleanup()
{
    if [ -n "$KEEPER_PID" ] && kill -0 "$KEEPER_PID" 2>/dev/null; then
        kill "$KEEPER_PID" 2>/dev/null
        wait "$KEEPER_PID" 2>/dev/null
    fi
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# `http_control.port` cannot be 0: Keeper reports the configured port, not the one the kernel would
# pick. So a port is drawn at random and a collision retried - binding is the only reliable test of
# a free port, and probe-then-bind is a race the concurrent copies of this test would lose. Drawn
# below the local ephemeral range, where outgoing connections and their minute in TIME_WAIT live.
read -r EPH_LOW _ <<< "$(cat /proc/sys/net/ipv4/ip_local_port_range 2>/dev/null)"
PORT_COUNT=$(( ${EPH_LOW:-32768} > 2048 ? ${EPH_LOW:-32768} - 1024 : 64512 ))

function stop_keeper()
{
    exec 9<&-
    kill "$KEEPER_PID" 2>/dev/null
    wait "$KEEPER_PID" 2>/dev/null
    KEEPER_PID=""
}

# $1 = MALLOC_CONF of the Keeper, appended to the one of the environment. Sets PORT and KEEPER_PID.
function start_keeper()
{
    local attempt line ready
    for attempt in $(seq 1 20); do
        rm -rf "${TMP_DIR}/coordination" "${TMP_DIR}/keeper.log" "$CONSOLE"
        PORT=$((1024 + RANDOM % PORT_COUNT))
        cat > "${TMP_DIR}/keeper_config.xml" <<EOF
<clickhouse>
    <listen_host>127.0.0.1</listen_host>
    <logger><level>information</level><log>${TMP_DIR}/keeper.log</log><console>1</console></logger>
    <keeper_server>
        <server_id>1</server_id><create_snapshot_on_exit>false</create_snapshot_on_exit>
        <log_storage_path>${TMP_DIR}/coordination/log</log_storage_path>
        <snapshot_storage_path>${TMP_DIR}/coordination/snapshots</snapshot_storage_path>
        <http_control><port>${PORT}</port></http_control>
        <coordination_settings><heart_beat_interval_ms>20</heart_beat_interval_ms>
            <election_timeout_lower_bound_ms>40</election_timeout_lower_bound_ms>
            <election_timeout_upper_bound_ms>80</election_timeout_upper_bound_ms>
            <force_sync>false</force_sync><raft_logs_level>warning</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
            <server><id>1</id><hostname>127.0.0.1</hostname><port>0</port></server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
EOF
        mkfifo "$CONSOLE"
        MALLOC_CONF="${MALLOC_CONF:+${MALLOC_CONF},}$1" \
            "$CLICKHOUSE_BINARY" keeper --config="${TMP_DIR}/keeper_config.xml" > "$CONSOLE" 2>&1 &
        KEEPER_PID=$!
        # Keeper's own console output is the wait: `read` returns on the next line and on the exit
        # that a taken port causes, so neither outcome costs a wait of a fixed length. The reading
        # end is held open until the process is gone, so Keeper never writes into a closed pipe.
        ready=""
        exec 9< "$CONSOLE"
        while IFS= read -r -t 30 -u 9 line; do
            case "$line" in *'Ready for connections.'*) ready=1; break ;; esac
        done
        [ -n "$ready" ] && return 0
        stop_keeper
    done
    return 1
}

function report_status()
{
    echo "--- $1"
    if ! start_keeper "$2"; then
        echo 'FAIL: no Keeper reached the control listener'
        return
    fi
    echo "GET $(${CLICKHOUSE_CURL} --max-time 10 -H 'Connection: close' -o "${TMP_DIR}/body" \
        -w '%{http_code}' "http://127.0.0.1:${PORT}/jemalloc/status")"
    echo "HEAD $(${CLICKHOUSE_CURL} --max-time 10 -I -H 'Connection: close' -o /dev/null \
        -w '%{http_code} %{size_download}' "http://127.0.0.1:${PORT}/jemalloc/status")"
    ${CLICKHOUSE_CLIENT} --param_j="$(cat "${TMP_DIR}/body")" --query "
        SELECT format('prof_enabled={} prof_active={} thread_active_init={} lg_sample={} errors={}',
            JSONExtractBool({j:String}, 'prof_enabled'), JSONExtractBool({j:String}, 'prof_active'),
            JSONExtractBool({j:String}, 'thread_active_init'), JSONExtractUInt({j:String}, 'lg_sample'),
            JSONHas({j:String}, 'errors'))"
    stop_keeper
}

# Armed: every field is what the process was configured with, not a constant. With `opt.prof` off:
# the three switches off and no sampling, rather than left out of the document or reported unknown.
report_status 'armed' 'prof:true,prof_active:true,prof_thread_active_init:true,lg_prof_sample:20'
report_status 'off' 'prof:false'
