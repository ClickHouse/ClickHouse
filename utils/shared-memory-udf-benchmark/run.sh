#!/usr/bin/env bash
# Benchmark of the executable-UDF data transports: pipes vs shared memory. All UDFs are
# functionally identical echoes (see functions.xml / user_scripts), so
# wall-clock differences are attributable to the transport alone.
#
# It runs each variant with clickhouse-local, reports the median query time over several iterations
# and the amount of data that crossed the kernel via read()/write() syscalls (OSReadChars /
# OSWriteChars) — the latter is a build-independent structural metric of the transport.
#
# Usage:
#   ./run.sh [--clickhouse PATH] [--rows N] [--row-bytes B] [--iters K] [--threads T]
#
# Environment: CLICKHOUSE may point at the binary instead of --clickhouse.
set -euo pipefail
export LC_ALL=C

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CLICKHOUSE="${CLICKHOUSE:-}"
ROWS=1000000
ROW_BYTES=100
ITERS=7
THREADS=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --clickhouse) CLICKHOUSE="$2"; shift 2 ;;
        --rows)       ROWS="$2"; shift 2 ;;
        --row-bytes)  ROW_BYTES="$2"; shift 2 ;;
        --iters)      ITERS="$2"; shift 2 ;;
        --threads)    THREADS="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$CLICKHOUSE" ]]; then
    for c in clickhouse "$HERE/../../build/programs/clickhouse"; do
        if command -v "$c" >/dev/null 2>&1 || [[ -x "$c" ]]; then CLICKHOUSE="$c"; break; fi
    done
fi
[[ -n "$CLICKHOUSE" ]] || { echo "clickhouse binary not found; pass --clickhouse PATH" >&2; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/user_scripts" "$WORK/state"
cp "$HERE"/user_scripts/*.py "$WORK/user_scripts/"
chmod +x "$WORK"/user_scripts/*.py
cp "$HERE/functions.xml" "$WORK/functions.xml"
cat > "$WORK/config.xml" <<EOF
<clickhouse>
    <path>$WORK/state</path>
    <user_scripts_path>$WORK/user_scripts/</user_scripts_path>
    <user_defined_executable_functions_config>$WORK/functions.xml</user_defined_executable_functions_config>
</clickhouse>
EOF

query_for() {
    local fn="$1"
    echo "SELECT sum(length($fn(val))) FROM (SELECT leftPad(toString(number), $ROW_BYTES, '0') AS val FROM numbers($ROWS)) SETTINGS max_threads = $THREADS"
}

run_once() { # prints elapsed seconds (from --time, last stderr line)
    local fn="$1"
    "$CLICKHOUSE" local --config-file "$WORK/config.xml" --time \
        --query "$(query_for "$fn")" 2> "$WORK/t.err" 1> /dev/null
    tail -n 1 "$WORK/t.err"
}

median() { # median of stdin numbers
    sort -g | awk '{a[NR]=$1} END{ if(NR%2) print a[(NR+1)/2]; else printf "%.4f\n",(a[NR/2]+a[NR/2+1])/2 }'
}

syscall_io() { # prints "readMB writeMB" for one run with profile events
    local fn="$1"
    "$CLICKHOUSE" local --config-file "$WORK/config.xml" --print-profile-events \
        --query "$(query_for "$fn")" 2> "$WORK/p.err" 1> /dev/null
    awk '/OSReadChars:/{r+=$(NF-1)} /OSWriteChars:/{w+=$(NF-1)}
         END{printf "%.2f %.2f\n", r/1048576, w/1048576}' "$WORK/p.err"
}

echo "clickhouse : $CLICKHOUSE"
echo "workload   : $ROWS rows x $ROW_BYTES bytes, max_threads=$THREADS, iters=$ITERS (median), warmup dropped"
echo
printf "%-26s %12s %14s %14s\n" "transport" "median, s" "read via sc" "write via sc"
printf "%-26s %12s %14s %14s\n" "--------------------------" "---------" "-----------" "------------"

for fn in bench_pipe_stream bench_pipe_chunk bench_shm bench_shm_busy; do
    run_once "$fn" >/dev/null                       # warmup (dropped)
    times=""
    for _ in $(seq 1 "$ITERS"); do times+="$(run_once "$fn")"$'\n'; done
    med="$(printf '%s' "$times" | median)"
    read -r rmb wmb < <(syscall_io "$fn") || true
    printf "%-26s %12s %11s MB %11s MB\n" "$fn" "$med" "$rmb" "$wmb"
done
