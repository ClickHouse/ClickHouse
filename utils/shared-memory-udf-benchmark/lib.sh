# Shared by run.sh and matrix.sh: a throw-away ClickHouse server with the benchmark functions,
# started once and queried through clickhouse-client.
#
# A server rather than clickhouse-local, because the thing being measured is a *pooled* transport.
# clickhouse-local lives for one query, so every sample would start its worker processes afresh
# and, for shared memory, reserve their regions afresh - a cost a long-lived server pays once per
# worker, not once per query. With a server the first query per function warms its pool and every
# sample after that measures the steady state.
#
# Expects CLICKHOUSE and HERE to be set. Provides: bench_start_server, bench_query (runs a query,
# stdout discarded, returns non-zero on failure), bench_time (prints elapsed seconds),
# bench_syscall_io (prints "readMB writeMB"). The server is stopped on exit.

BENCH_WORK="$(mktemp -d)"
BENCH_PORT=""
BENCH_SERVER_PID=""

bench_cleanup() {
    if [[ -n "$BENCH_SERVER_PID" ]]; then
        kill "$BENCH_SERVER_PID" 2>/dev/null || true
        wait "$BENCH_SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$BENCH_WORK"
}
trap bench_cleanup EXIT

bench_start_server() {
    mkdir -p "$BENCH_WORK/user_scripts" "$BENCH_WORK/state"
    cp "$HERE"/user_scripts/*.py "$BENCH_WORK/user_scripts/"
    chmod +x "$BENCH_WORK"/user_scripts/*.py
    cp "$HERE/functions.xml" "$BENCH_WORK/functions.xml"

    # A free port: bind one and let go of it. There is a window in which something else could take
    # it, and the readiness wait below reports that as a server that never came up.
    BENCH_PORT="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"

    cat > "$BENCH_WORK/config.xml" <<XML
<clickhouse>
    <logger>
        <level>warning</level>
        <console>0</console>
        <log>$BENCH_WORK/server.log</log>
        <errorlog>$BENCH_WORK/server.err.log</errorlog>
    </logger>
    <listen_host>127.0.0.1</listen_host>
    <tcp_port>$BENCH_PORT</tcp_port>
    <path>$BENCH_WORK/state/</path>
    <tmp_path>$BENCH_WORK/state/tmp/</tmp_path>
    <user_scripts_path>$BENCH_WORK/user_scripts/</user_scripts_path>
    <user_defined_executable_functions_config>$BENCH_WORK/functions.xml</user_defined_executable_functions_config>
    <mark_cache_size>1048576</mark_cache_size>
    <profiles><default/></profiles>
    <users>
        <default>
            <password/>
            <networks><ip>127.0.0.1</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <quotas><default/></quotas>
</clickhouse>
XML

    # No watchdog: the server is then the process started here, so it can be signalled directly and
    # the worker processes of the functions are its immediate children.
    CLICKHOUSE_WATCHDOG_ENABLE=0 "$CLICKHOUSE" server --config-file "$BENCH_WORK/config.xml" > "$BENCH_WORK/server.stdout" 2>&1 &
    BENCH_SERVER_PID=$!

    local i
    for i in $(seq 1 100); do
        if "$CLICKHOUSE" client --host 127.0.0.1 --port "$BENCH_PORT" --query "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "$BENCH_SERVER_PID" 2>/dev/null; then
            break
        fi
        sleep 0.1
    done
    echo "the benchmark server did not come up:" >&2
    cat "$BENCH_WORK/server.stdout" "$BENCH_WORK/server.err.log" 2>/dev/null >&2
    return 1
}

bench_client() {
    "$CLICKHOUSE" client --host 127.0.0.1 --port "$BENCH_PORT" "$@"
}

# Runs a query and discards its result; the server's error goes to stderr. Used to warm a pool.
bench_query() {
    bench_client --query "$1" >/dev/null
}

# Prints the elapsed seconds of one run, as the client reports them with --time (the last line of
# its stderr). Anything but a number there means the run printed something else, and that must not
# become a sample: `set -e` does not reach into the command substitution this is called from, so it
# is said out loud here.
bench_time() {
    local elapsed
    if ! bench_client --time --query "$1" 2> "$BENCH_WORK/t.err" 1> /dev/null; then
        echo "run failed:" >&2
        cat "$BENCH_WORK/t.err" >&2
        return 1
    fi
    elapsed="$(tail -n 1 "$BENCH_WORK/t.err")"
    if [[ ! "$elapsed" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
        echo "expected an elapsed time, got: $elapsed" >&2
        cat "$BENCH_WORK/t.err" >&2
        return 1
    fi
    printf '%s\n' "$elapsed"
}

# Prints "readMB writeMB": the bytes the server's threads moved through read()/write() during the
# query, from the OSReadChars/OSWriteChars profile events. Counted, not just summed: with no
# matching lines awk would print 0.00 0.00, and a renamed event would look like the perfect
# shared-memory result.
bench_syscall_io() {
    if ! bench_client --print-profile-events --query "$1" 2> "$BENCH_WORK/p.err" 1> /dev/null; then
        cat "$BENCH_WORK/p.err" >&2
        return 1
    fi
    awk '/OSReadChars:/{r+=$(NF-1); n++} /OSWriteChars:/{w+=$(NF-1); n++}
         END{ if (n < 2) exit 1; printf "%.2f %.2f\n", r/1048576, w/1048576}' "$BENCH_WORK/p.err" && return 0
    echo "no OSReadChars/OSWriteChars in the profile events - cannot report syscall I/O" >&2
    cat "$BENCH_WORK/p.err" >&2
    return 1
}

bench_median() { # median of stdin numbers
    sort -g | awk '{a[NR]=$1} END{ if(NR%2) print a[(NR+1)/2]; else printf "%.4f\n",(a[NR/2]+a[NR/2+1])/2 }'
}
