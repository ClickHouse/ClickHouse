#!/usr/bin/env bash
# Benchmark of the executable-UDF data transports: pipes vs shared memory. The transport variants
# are functionally identical echoes (see functions.xml / user_scripts), while bench_shm_busy adds
# artificial command-side CPU work and is reported separately.
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
    # `numbers_mt`, not `numbers`: the latter is a single stream no matter what `max_threads` says,
    # so with `--threads N` the query would still make one UDF call at a time and the thread setting
    # would measure nothing. With `max_threads = 1` the two are equivalent.
    echo "SELECT sum(length($fn(val))) FROM (SELECT leftPad(toString(number), $ROW_BYTES, '0') AS val FROM numbers_mt($ROWS)) SETTINGS max_threads = $THREADS"
}

run_once() { # prints elapsed seconds (from --time, last stderr line)
    local fn="$1" elapsed
    if ! "$CLICKHOUSE" local --config-file "$WORK/config.xml" --time \
        --query "$(query_for "$fn")" 2> "$WORK/t.err" 1> /dev/null; then
        echo "run of $fn failed:" >&2
        cat "$WORK/t.err" >&2
        return 1
    fi
    # Anything but a number here means the run printed something else to stderr and the "sample"
    # would be that text. `set -e` does not reach into the command substitution this is called
    # from, so say it out loud instead of measuring a diagnostic.
    elapsed="$(tail -n 1 "$WORK/t.err")"
    if [[ ! "$elapsed" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
        echo "expected an elapsed time for $fn, got: $elapsed" >&2
        cat "$WORK/t.err" >&2
        return 1
    fi
    printf '%s\n' "$elapsed"
}

median() { # median of stdin numbers
    sort -g | awk '{a[NR]=$1} END{ if(NR%2) print a[(NR+1)/2]; else printf "%.4f\n",(a[NR/2]+a[NR/2+1])/2 }'
}

syscall_io() { # prints "readMB writeMB" for one run with profile events
    local fn="$1"
    if ! "$CLICKHOUSE" local --config-file "$WORK/config.xml" --print-profile-events \
        --query "$(query_for "$fn")" 2> "$WORK/p.err" 1> /dev/null; then
        cat "$WORK/p.err" >&2
        return 1
    fi
    awk '/OSReadChars:/{r+=$(NF-1)} /OSWriteChars:/{w+=$(NF-1)}
         END{printf "%.2f %.2f\n", r/1048576, w/1048576}' "$WORK/p.err"
}

# Every shared-memory worker reserves its whole region with `posix_fallocate`, and one worker is
# borrowed per parallel UDF call, so `--threads N` needs N regions at once. A container /dev/shm
# (often 64 MiB) does not fit even one; say so before the run instead of failing halfway through it.
check_shared_memory_capacity() {
    local dir="/dev/shm" region workers required available
    region="$(grep -o '<shared_memory_size>[0-9]*' "$HERE/functions.xml" | head -1 | grep -o '[0-9]*')"
    [[ -n "$region" ]] || return 0
    workers="$1"
    required=$(( region * workers ))
    available="$(df -B1 --output=avail "$dir" 2>/dev/null | tail -n 1 | tr -d ' ')"
    [[ -n "$available" ]] || return 0
    if (( available < required )); then
        echo "not enough space in $dir: the benchmark needs $(( required / 1048576 )) MiB" \
             "($(( region / 1048576 )) MiB per worker x $workers), $(( available / 1048576 )) MiB available." >&2
        echo "Mount a bigger $dir (docker: --shm-size), lower <shared_memory_size> in functions.xml," \
             "or use fewer threads." >&2
        exit 1
    fi
}

check_shared_memory_capacity "$THREADS"

echo "clickhouse : $CLICKHOUSE"
echo "workload   : $ROWS rows x $ROW_BYTES bytes, max_threads=$THREADS, iters=$ITERS (median), warmup dropped"
echo
printf "%-26s %12s %14s %14s\n" "transport" "median, s" "read via sc" "write via sc"
printf "%-26s %12s %14s %14s\n" "--------------------------" "---------" "-----------" "------------"

for fn in bench_pipe_stream bench_pipe_chunk bench_shm bench_shm_busy; do
    run_once "$fn" >/dev/null                       # warmup (dropped)
    times=""
    for _ in $(seq 1 "$ITERS"); do
        sample="$(run_once "$fn")" || exit 1
        times+="$sample"$'\n'
    done
    med="$(printf '%s' "$times" | median)"
    io="$(syscall_io "$fn")" || exit 1
    read -r rmb wmb <<< "$io"
    printf "%-26s %12s %11s MB %11s MB\n" "$fn" "$med" "$rmb" "$wmb"
done
