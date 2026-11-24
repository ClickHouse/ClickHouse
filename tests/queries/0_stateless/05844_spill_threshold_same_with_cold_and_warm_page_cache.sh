#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: writes and reads a 160 MB table twice

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The memory available to the ratio spill thresholds, as printed by "Adjusting spill threshold": the same on a cold
# page cache and with a warm one, the query's `max_memory_usage` under `EXPLAIN ANALYZE` as without it, and a page
# cache that cannot shrink below its minimum still counted as used.

CONFIG_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_config.yaml"
LOG_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.log"
CACHE_SIZE=134217728

server_opts=(
    # we cannot use cgroups since this will sum RSS of all processes and clickhouse-local may fail with MEMORY_LIMIT_EXCEEDED
    --memory_worker_use_cgroup=0
    --memory_worker_dynamic_hard_limit=false
)

# A DISTINCT whose trace line is found by its ratio.
function probe()
{
    echo "SELECT DISTINCT number FROM numbers(100) SETTINGS max_bytes_ratio_before_external_distinct = $1,
        max_bytes_before_external_distinct = 0, send_logs_level = 'trace' $2 FORMAT Null;"
}

# Without merges nothing reads the table before the SELECT, so the first probe sees a cold page cache and the
# second a warm one.
function with_page_cache_filled()
{
    echo "
CREATE TABLE t (x UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t;
INSERT INTO t SELECT number FROM numbers(20000000);
SELECT value < 0.1 * ${CACHE_SIZE} FROM system.metrics WHERE metric = 'PageCacheBytes';
$(probe 0.41)
SELECT count() FROM t WHERE NOT ignore(x) FORMAT Null;
SELECT value > 0.8 * ${CACHE_SIZE} FROM system.metrics WHERE metric = 'PageCacheBytes';
$(probe 0.42)
"
}

function run_session()
{
    local page_cache_min_size=$1 queries=$2

    # memory_worker_correct_memory_tracker: false keeps the pages the allocator retains out of the compared values.
    > "${CONFIG_FILE}" echo "
max_server_memory_usage: 2147483648
page_cache_max_size: ${CACHE_SIZE}
page_cache_min_size: ${page_cache_min_size}
page_cache_free_memory_ratio: 0
page_cache_policy: LRU
page_cache_history_window_ms: 0
memory_worker_correct_memory_tracker: false
"

    $CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" --use_page_cache_for_local_disks 1 --local_filesystem_read_method pread \
        --multiquery "${queries}" -- "${server_opts[@]}" 2>"${LOG_FILE}"
}

# Prints 1 when the condition holds over the available memory of the probes with ratios r1 and r2 (in MiB), else 0.
function check()
{
    local name=$1 r1=$2 r2=$3 condition=$4
    awk -v name="${name}" -v r1="${r1}" -v r2="${r2}" '
        function to_mib(value, unit) {
            return value * (unit == "TiB" ? 1048576 : unit == "GiB" ? 1024 : unit == "MiB" ? 1 : unit == "KiB" ? 1 / 1024 : 1 / 1048576);
        }
        {
            if (!match($0, /max_bytes_ratio_before_external_distinct: [0-9.]+, available (system )?memory: [0-9.]+ [KMGT]?i?B/))
                next;
            n = split(substr($0, RSTART, RLENGTH), f, " ");
            ratio = f[2]; sub(/,$/, "", ratio);
            if (!(ratio in available))
                available[ratio] = to_mib(f[n - 1], f[n]);
        }
        END {
            if (!(r1 in available) || !(r2 in available)) { print name ": no trace line"; exit; }
            a = available[r1]; b = available[r2];
            print name ": " ('"${condition}"' ? 1 : 0);
        }' "${LOG_FILE}"
}

# The first query of a session also charges the server's lazy initialization to its own tracker. EXPLAIN ANALYZE
# prints its plan whatever the format.
run_session 0 "
SELECT DISTINCT number FROM numbers(100) FORMAT Null;
$(probe 0.31 ", max_memory_usage = '512Mi'")
EXPLAIN ANALYZE $(probe 0.32 ", max_memory_usage = '512Mi'")
" > /dev/null
check "query limit" 0.31 0.32 "a <= 512 && b <= 512 && a - b < 16 && b - a < 16"

run_session 0 "$(with_page_cache_filled)"
check "cold and warm page cache" 0.41 0.42 "a - b < ${CACHE_SIZE} / 4 / 1048576 && b - a < ${CACHE_SIZE} / 4 / 1048576"

# A cache that cannot shrink below its minimum stays counted as used.
run_session ${CACHE_SIZE} "$(with_page_cache_filled)"
check "page cache below its minimum" 0.41 0.42 "a - b > ${CACHE_SIZE} * 3 / 4 / 1048576"

rm "${CONFIG_FILE}" "${LOG_FILE}"
