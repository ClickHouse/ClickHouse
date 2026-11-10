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

# Runs one (fn, rows, rowbytes, block, threads) point and writes its samples to the given file.
sample_point() {
    local fn=$1 rows=$2 rb=$3 blk=$4 th=$5 out=$6
    # `numbers_mt`, not `numbers`: the latter is a single stream whatever `max_threads` says, so the
    # thread sweep below would run one UDF call at a time in every row of its table. With
    # `max_threads=1` the two are equivalent.
    local q="SELECT sum(length($fn(val))) FROM (SELECT leftPad(toString(number),$rb,'0') AS val FROM numbers_mt($rows)) SETTINGS max_threads=$th, max_block_size=$blk"
    # The first run warms the function's pool up to this thread count (a wider sweep step starts
    # more workers) and is not a sample. A failing run must not turn into one either: neither
    # `set -e` nor this function's own status reaches the caller through the command substitution
    # it is called from, so every run is checked here.
    bench_query "$q" || { echo "warm-up of $fn failed" >&2; return 1; }
    : > "$out"
    local sample
    for _ in $(seq 1 "$ITERS"); do
        sample="$(bench_time "$q")" || { echo "run of $fn failed" >&2; return 1; }
        printf '%s\n' "$sample" >> "$out"
    done
}

# Prints one table row: pipe median [q1-q3], shm median [q1-q3], speedup [95% CI].
compare_point() {
    local label=$1; shift
    sample_point bench_pipe_chunk "$@" "$BENCH_WORK/pipe.samples" || return 1
    sample_point bench_shm        "$@" "$BENCH_WORK/shm.samples"  || return 1
    local pm pq1 pq3 sm sq1 sq3 ratio lo hi
    read -r pm pq1 pq3 <<< "$("$HERE/stats.py" summary "$BENCH_WORK/pipe.samples")"
    read -r sm sq1 sq3 ratio lo hi <<< "$("$HERE/stats.py" ratio "$BENCH_WORK/pipe.samples" "$BENCH_WORK/shm.samples")"
    printf "%-18s %8s %-15s %8s %-15s %6sx %-13s\n" "$label" "$pm" "[$pq1-$pq3]" "$sm" "[$sq1-$sq3]" "$ratio" "[$lo-$hi]"
}

header() {
    printf "%-18s %8s %-15s %8s %-15s %7s %-13s\n" "$1" "pipe,s" "[q1-q3]" "shm,s" "[q1-q3]" "speedup" "[95% CI]"
}


echo "clickhouse: $CLICKHOUSE  (iters=$ITERS; median, interquartile range, speedup = ratio of medians with 95% bootstrap CI)"
echo

echo "### 1. Block-size sweep (rows=2M, row=100B, threads=1)"
header "max_block"
for blk in 8192 16384 32768 65536 131072; do
    compare_point "$blk" 2000000 100 "$blk" 1 || exit 1
done
echo

echo "### 2. Thread sweep (rows=4M, row=100B, block=65536)"
header "threads"
for th in 1 2 4 8 16; do
    compare_point "$th" 4000000 100 65536 "$th" || exit 1
done
echo

echo "### 3. Row-size sweep (~200MB total, threads=1, block=65536)"
header "rows x bytes"
for pair in "20000000 10" "2000000 100" "200000 1000"; do
    set -- $pair; rows=$1; rb=$2
    compare_point "${rows}x${rb}" "$rows" "$rb" 65536 1 || exit 1
done
