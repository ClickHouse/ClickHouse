#!/usr/bin/env bash
# Sweeps for the executable-UDF transport benchmark: block size, thread count and row size.
# Reports median query time (from --time) for the fair per-chunk pipe baseline vs shared memory,
# and their ratio. See run.sh for the single-point benchmark and README.md for details.
set -euo pipefail
export LC_ALL=C

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLICKHOUSE="${CLICKHOUSE:-$HERE/../../build/programs/clickhouse}"
ITERS="${ITERS:-7}"
[[ -x "$CLICKHOUSE" ]] || { echo "clickhouse not found at $CLICKHOUSE (set CLICKHOUSE=...)"; exit 1; }

WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/user_scripts" "$WORK/state"
cp "$HERE"/user_scripts/*.py "$WORK/user_scripts/"; chmod +x "$WORK"/user_scripts/*.py
cp "$HERE/functions.xml" "$WORK/functions.xml"
cat > "$WORK/config.xml" <<EOF
<clickhouse>
    <path>$WORK/state</path>
    <user_scripts_path>$WORK/user_scripts/</user_scripts_path>
    <user_defined_executable_functions_config>$WORK/functions.xml</user_defined_executable_functions_config>
</clickhouse>
EOF

# median seconds for one (fn, rows, rowbytes, block, threads)
median_time() {
    local fn=$1 rows=$2 rb=$3 blk=$4 th=$5
    local q="SELECT sum(length($fn(val))) FROM (SELECT leftPad(toString(number),$rb,'0') AS val FROM numbers($rows)) SETTINGS max_threads=$th, max_block_size=$blk"
    "$CLICKHOUSE" local --config-file "$WORK/config.xml" --query "$q" >/dev/null 2>&1 || true  # warmup
    local ts=()
    for _ in $(seq 1 "$ITERS"); do
        "$CLICKHOUSE" local --config-file "$WORK/config.xml" --time --query "$q" 2>"$WORK/e" 1>/dev/null
        ts+=("$(tail -n1 "$WORK/e")")
    done
    printf '%s\n' "${ts[@]}" | sort -g | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:(a[NR/2]+a[NR/2+1])/2}'
}

ratio() { awk -v a="$1" -v b="$2" 'BEGIN{ if(b>0) printf "%.2f", a/b; else print "-" }'; }

echo "clickhouse: $CLICKHOUSE  (iters=$ITERS, median)"
echo

echo "### 1. Block-size sweep (rows=2M, row=100B, threads=1)"
printf "%-12s %12s %12s %10s\n" "max_block" "pipe_chunk,s" "shm,s" "speedup"
for blk in 8192 16384 32768 65536 131072; do
    p=$(median_time bench_pipe_chunk 2000000 100 "$blk" 1)
    s=$(median_time bench_shm        2000000 100 "$blk" 1)
    printf "%-12s %12s %12s %9sx\n" "$blk" "$p" "$s" "$(ratio "$p" "$s")"
done
echo

echo "### 2. Thread sweep (rows=4M, row=100B, block=65536)"
printf "%-12s %12s %12s %10s\n" "threads" "pipe_chunk,s" "shm,s" "speedup"
for th in 1 2 4 8 16; do
    p=$(median_time bench_pipe_chunk 4000000 100 65536 "$th")
    s=$(median_time bench_shm        4000000 100 65536 "$th")
    printf "%-12s %12s %12s %9sx\n" "$th" "$p" "$s" "$(ratio "$p" "$s")"
done
echo

echo "### 3. Row-size sweep (~200MB total, threads=1, block=65536)"
printf "%-18s %12s %12s %10s\n" "rows x bytes" "pipe_chunk,s" "shm,s" "speedup"
for pair in "20000000 10" "2000000 100" "200000 1000"; do
    set -- $pair; rows=$1; rb=$2
    p=$(median_time bench_pipe_chunk "$rows" "$rb" 65536 1)
    s=$(median_time bench_shm        "$rows" "$rb" 65536 1)
    printf "%-18s %12s %12s %9sx\n" "${rows}x${rb}" "$p" "$s" "$(ratio "$p" "$s")"
done
