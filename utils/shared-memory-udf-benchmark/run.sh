#!/usr/bin/env bash
# Benchmark of the executable-UDF data transports: pipes vs shared memory. The transport variants
# are functionally identical echoes (see functions.xml / user_scripts), while bench_shm_busy adds
# artificial command-side CPU work and is reported separately.
#
# It runs each variant against a throw-away server (see lib.sh for why not clickhouse-local),
# reports the median query time over several iterations and the amount of data that crossed the
# kernel via read()/write() syscalls (OSReadChars / OSWriteChars) — the latter is a
# build-independent structural metric of the transport.
#
# Usage:
#   ./run.sh [--clickhouse PATH] [--rows N] [--row-bytes B] [--iters K] [--threads T]
#            [--block-size S]
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
# Pinned rather than left to the server default: the block-size sweep in README.md moves the
# pipe-vs-shared-memory ratio from 2.06x to 1.19x, so an unstated block size changes the headline
# result without anything about the transport changing.
BLOCK_SIZE=65536

while [[ $# -gt 0 ]]; do
    case "$1" in
        --clickhouse) CLICKHOUSE="$2"; shift 2 ;;
        --rows)       ROWS="$2"; shift 2 ;;
        --row-bytes)  ROW_BYTES="$2"; shift 2 ;;
        --iters)      ITERS="$2"; shift 2 ;;
        --threads)    THREADS="$2"; shift 2 ;;
        --block-size) BLOCK_SIZE="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$CLICKHOUSE" ]]; then
    for c in clickhouse "$HERE/../../build/programs/clickhouse"; do
        if command -v "$c" >/dev/null 2>&1 || [[ -x "$c" ]]; then CLICKHOUSE="$c"; break; fi
    done
fi
[[ -n "$CLICKHOUSE" ]] || { echo "clickhouse binary not found; pass --clickhouse PATH" >&2; exit 1; }

# shellcheck source=lib.sh
source "$HERE/lib.sh"
bench_start_server

query_for() {
    local fn="$1"
    # `numbers_mt`, not `numbers`: the latter is a single stream no matter what `max_threads` says,
    # so with `--threads N` the query would still make one UDF call at a time and the thread setting
    # would measure nothing. With `max_threads = 1` the two are equivalent.
    echo "SELECT sum(length($fn(val))) FROM (SELECT leftPad(toString(number), $ROW_BYTES, '0') AS val FROM numbers_mt($ROWS)) SETTINGS max_threads = $THREADS, max_block_size = $BLOCK_SIZE"
}

echo "clickhouse : $CLICKHOUSE"
echo "workload   : $ROWS rows x $ROW_BYTES bytes, max_threads=$THREADS, max_block_size=$BLOCK_SIZE, iters=$ITERS (median, interquartile range, speedup with 95% bootstrap CI) after one warm-up query per function"
echo
printf "%-26s %10s %14s %20s %12s %12s\n" "transport" "median, s" "IQR, s" "vs pipe baseline" "read via sc" "write via sc"
printf "%-26s %10s %14s %20s %12s %12s\n" "--------------------------" "---------" "-------------" "--------------------" "-----------" "------------"

# The samples of every transport are kept, so that the shared-memory result can be compared with
# the fair pipe baseline as a ratio of medians with a bootstrap confidence interval (stats.py),
# rather than as two numbers the reader has to compare by eye.
for fn in bench_pipe_chunk bench_pipe_stream bench_pipe_chunk_1m bench_shm bench_pipe_busy bench_shm_busy; do
    # The first query starts the function's pool - the worker processes and, for shared memory,
    # their regions. It is not a sample: that is the cost a server pays once per worker.
    bench_query "$(query_for "$fn")" || exit 1
    : > "$BENCH_WORK/$fn.samples"
    for _ in $(seq 1 "$ITERS"); do
        sample="$(bench_time "$(query_for "$fn")")" || exit 1
        printf '%s\n' "$sample" >> "$BENCH_WORK/$fn.samples"
    done
    # Each variant is compared with the pipe function that does the same work: the echoes with
    # bench_pipe_chunk, the busy command with bench_pipe_busy. Comparing a command that does
    # artificial work with one that does none would measure the work, not the transport.
    case "$fn" in
        bench_pipe_chunk) baseline="" ;;
        bench_shm_busy)   baseline="bench_pipe_busy" ;;
        bench_pipe_busy)  baseline="" ;;
        *)                baseline="bench_pipe_chunk" ;;
    esac
    if [[ -z "$baseline" ]]; then
        read -r med q1 q3 <<< "$("$HERE/stats.py" summary "$BENCH_WORK/$fn.samples")"
        speedup="1 (baseline)"
    else
        read -r med q1 q3 ratio lo hi <<< "$("$HERE/stats.py" ratio "$BENCH_WORK/$baseline.samples" "$BENCH_WORK/$fn.samples")"
        speedup="${ratio}x [$lo-$hi]"
    fi
    io="$(bench_syscall_io "$(query_for "$fn")")" || exit 1
    read -r rmb wmb <<< "$io"
    printf "%-26s %10s %14s %20s %9s MB %9s MB\n" "$fn" "$med" "$q1-$q3" "$speedup" "$rmb" "$wmb"
done
