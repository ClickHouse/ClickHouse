#!/usr/bin/env bash
# The memory side of the trade: what a warm pool costs while it sits idle, pipe against shared
# memory. Warms each function's pool to --threads workers, then reads what the server's memory
# tracker charges, what the process has resident, and how much shared memory the system holds -
# the last one is where the regions' pages live (they are memfd pages, not part of the server's
# heap, and only the ones the server touched show in its RSS).
#
# Usage: ./idle_memory.sh [--clickhouse PATH] [--threads T]
set -euo pipefail
export LC_ALL=C

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLICKHOUSE="${CLICKHOUSE:-}"
THREADS=16

while [[ $# -gt 0 ]]; do
    case "$1" in
        --clickhouse) CLICKHOUSE="$2"; shift 2 ;;
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

# shellcheck source=lib.sh
source "$HERE/lib.sh"
bench_start_server

metric() { # tracked / resident / pooled shared-memory bytes, and the system's Shmem, in MiB
    local tracked resident pooled shmem
    tracked="$(bench_client --query "SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'")"
    pooled="$(bench_client --query "SELECT value FROM system.metrics WHERE metric = 'ExecutableUDFSharedMemoryPooledBytes'")"
    resident="$(awk '/^VmRSS:/ {print $2 * 1024}' "/proc/$BENCH_SERVER_PID/status")"
    shmem="$(awk '/^Shmem:/ {print $2 * 1024}' /proc/meminfo)"
    printf '%8.1f %8.1f %8.1f %8.1f\n' "$((tracked))e-6" "$((resident))e-6" "$((pooled))e-6" "$((shmem))e-6" \
        | awk '{printf "%8.1f %8.1f %8.1f %8.1f\n", $1/1.048576, $2/1.048576, $3/1.048576, $4/1.048576}'
}

query_for() {
    echo "SELECT sum(length($1(val))) FROM (SELECT leftPad(toString(number), 100, '0') AS val FROM numbers_mt(1000000)) SETTINGS max_threads = $THREADS, max_block_size = 65536"
}

echo "clickhouse : $CLICKHOUSE"
echo "pools      : $THREADS workers each, warmed and then left idle; MiB"
echo
printf "%-34s %8s %8s %8s %8s\n" "state" "tracked" "RSS" "pooled" "Shmem"
m0="$(metric)"; printf "%-34s %8s\n" "fresh server" "$m0"
bench_query "$(query_for bench_pipe_chunk)"
sleep 1
m1="$(metric)"; printf "%-34s %8s\n" "+ pipe pool ($THREADS workers)" "$m1"
bench_query "$(query_for bench_shm)"
sleep 1
m2="$(metric)"; printf "%-34s %8s\n" "+ shared-memory pool ($THREADS workers)" "$m2"
delta() { awk -v a="$1" -v b="$2" 'BEGIN{split(a,x," "); split(b,y," "); printf "%+8.1f %+8.1f %+8.1f %+8.1f", y[1]-x[1], y[2]-x[2], y[3]-x[3], y[4]-x[4]}'; }
printf "%-34s %8s\n" "delta: pipe pool" "$(delta "$m0" "$m1")"
printf "%-34s %8s\n" "delta: shared-memory pool" "$(delta "$m1" "$m2")"
echo
echo "The 'tracked' delta of the shared-memory step is the idle charge (pool_size x shared_memory_size,"
echo "16 MiB per worker here); 'Shmem' is where those pages actually live. The pipe pool costs only"
echo "its worker processes, which neither column sees - they are the workers' memory, not the server's."
