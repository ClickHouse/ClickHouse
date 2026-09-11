#!/usr/bin/env bash
# Sweeps for the executable-UDF transport benchmark: block size, thread count and row size.
# Reports median query time (from --time) for the fair per-chunk pipe baseline vs shared memory,
# and their ratio, against a warm server (see lib.sh). See run.sh for the single-point benchmark
# and README.md for details.
set -euo pipefail
export LC_ALL=C

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLICKHOUSE="${CLICKHOUSE:-$HERE/../../build/programs/clickhouse}"
ITERS="${ITERS:-7}"
[[ -x "$CLICKHOUSE" ]] || { echo "clickhouse not found at $CLICKHOUSE (set CLICKHOUSE=...)"; exit 1; }

# shellcheck source=lib.sh
source "$HERE/lib.sh"
bench_start_server

# median seconds for one (fn, rows, rowbytes, block, threads)
median_time() {
    local fn=$1 rows=$2 rb=$3 blk=$4 th=$5
    # `numbers_mt`, not `numbers`: the latter is a single stream whatever `max_threads` says, so the
    # thread sweep below would run one UDF call at a time in every row of its table. With
    # `max_threads=1` the two are equivalent.
    local q="SELECT sum(length($fn(val))) FROM (SELECT leftPad(toString(number),$rb,'0') AS val FROM numbers_mt($rows)) SETTINGS max_threads=$th, max_block_size=$blk"
    # The first run warms the function's pool up to this thread count (a wider sweep step starts
    # more workers) and is not a sample. A failing run must not turn into one either: neither
    # `set -e` nor this function's own status reaches the caller through the command substitution
    # it is called from, so every run is checked here.
    bench_query "$q" || { echo "warm-up of $fn failed" >&2; return 1; }
    local ts=() sample
    for _ in $(seq 1 "$ITERS"); do
        sample="$(bench_time "$q")" || { echo "run of $fn failed" >&2; return 1; }
        ts+=("$sample")
    done
    printf '%s\n' "${ts[@]}" | bench_median
}

ratio() { awk -v a="$1" -v b="$2" 'BEGIN{ if(b>0) printf "%.2f", a/b; else print "-" }'; }


echo "clickhouse: $CLICKHOUSE  (iters=$ITERS, median)"
echo

echo "### 1. Block-size sweep (rows=2M, row=100B, threads=1)"
printf "%-12s %12s %12s %10s\n" "max_block" "pipe_chunk,s" "shm,s" "speedup"
for blk in 8192 16384 32768 65536 131072; do
    p=$(median_time bench_pipe_chunk 2000000 100 "$blk" 1) || exit 1
    s=$(median_time bench_shm        2000000 100 "$blk" 1) || exit 1
    printf "%-12s %12s %12s %9sx\n" "$blk" "$p" "$s" "$(ratio "$p" "$s")"
done
echo

echo "### 2. Thread sweep (rows=4M, row=100B, block=65536)"
printf "%-12s %12s %12s %10s\n" "threads" "pipe_chunk,s" "shm,s" "speedup"
for th in 1 2 4 8 16; do
    p=$(median_time bench_pipe_chunk 4000000 100 65536 "$th") || exit 1
    s=$(median_time bench_shm        4000000 100 65536 "$th") || exit 1
    printf "%-12s %12s %12s %9sx\n" "$th" "$p" "$s" "$(ratio "$p" "$s")"
done
echo

echo "### 3. Row-size sweep (~200MB total, threads=1, block=65536)"
printf "%-18s %12s %12s %10s\n" "rows x bytes" "pipe_chunk,s" "shm,s" "speedup"
for pair in "20000000 10" "2000000 100" "200000 1000"; do
    set -- $pair; rows=$1; rb=$2
    p=$(median_time bench_pipe_chunk "$rows" "$rb" 65536 1) || exit 1
    s=$(median_time bench_shm        "$rows" "$rb" 65536 1) || exit 1
    printf "%-18s %12s %12s %9sx\n" "${rows}x${rb}" "$p" "$s" "$(ratio "$p" "$s")"
done
