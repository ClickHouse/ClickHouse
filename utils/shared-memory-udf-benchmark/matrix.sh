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

# Every shared-memory worker reserves its whole region with `posix_fallocate`, and one worker is
# borrowed per parallel UDF call, so the thread sweep needs as many regions as its widest point. A
# container /dev/shm (often 64 MiB) does not fit even one; say so before the sweep rather than
# failing somewhere in the middle of it.
check_shared_memory_capacity() {
    local dir="/dev/shm" region workers required available
    region="$(grep -o '<shared_memory_size>[0-9]*' "$HERE/functions.xml" | head -1 | grep -o '[0-9]*')"
    [[ -n "$region" ]] || return 0
    workers="$1"
    required=$(( region * workers ))
    available="$(df -B1 --output=avail "$dir" 2>/dev/null | tail -n 1 | tr -d ' ')"
    [[ -n "$available" ]] || return 0
    if (( available < required )); then
        echo "not enough space in $dir: the sweep needs $(( required / 1048576 )) MiB" \
             "($(( region / 1048576 )) MiB per worker x $workers), $(( available / 1048576 )) MiB available." >&2
        echo "Mount a bigger $dir (docker: --shm-size), lower <shared_memory_size> in functions.xml," \
             "or shorten the thread sweep." >&2
        exit 1
    fi
}

# median seconds for one (fn, rows, rowbytes, block, threads)
median_time() {
    local fn=$1 rows=$2 rb=$3 blk=$4 th=$5
    # `numbers_mt`, not `numbers`: the latter is a single stream whatever `max_threads` says, so the
    # thread sweep below would run one UDF call at a time in every row of its table. With
    # `max_threads=1` the two are equivalent.
    local q="SELECT sum(length($fn(val))) FROM (SELECT leftPad(toString(number),$rb,'0') AS val FROM numbers_mt($rows)) SETTINGS max_threads=$th, max_block_size=$blk"
    # A failing run must not turn into a sample: neither `set -e` nor this function's own status
    # reaches the caller through the command substitution it is called from, so every run is
    # checked here and every sample is validated to be a number.
    if ! "$CLICKHOUSE" local --config-file "$WORK/config.xml" --query "$q" >/dev/null 2>"$WORK/e"; then
        echo "warm-up of $fn failed:" >&2; cat "$WORK/e" >&2; return 1
    fi
    local ts=() sample
    for _ in $(seq 1 "$ITERS"); do
        if ! "$CLICKHOUSE" local --config-file "$WORK/config.xml" --time --query "$q" 2>"$WORK/e" 1>/dev/null; then
            echo "run of $fn failed:" >&2; cat "$WORK/e" >&2; return 1
        fi
        sample="$(tail -n1 "$WORK/e")"
        if [[ ! "$sample" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
            echo "expected an elapsed time for $fn, got: $sample" >&2; cat "$WORK/e" >&2; return 1
        fi
        ts+=("$sample")
    done
    printf '%s\n' "${ts[@]}" | sort -g | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:(a[NR/2]+a[NR/2+1])/2}'
}

ratio() { awk -v a="$1" -v b="$2" 'BEGIN{ if(b>0) printf "%.2f", a/b; else print "-" }'; }

# The widest point of the sweeps below is the 16-thread step.
check_shared_memory_capacity 16

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
