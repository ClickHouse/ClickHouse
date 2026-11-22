#!/usr/bin/env bash
# Tags: no-fasttest, no-asan, no-tsan, no-msan
# Without jemalloc the memory worker sizes the page cache by the process RSS, which does not fit the 640 MiB limit.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The userspace page cache above `page_cache_min_size` counts as available memory for the ratio spill thresholds:
# with the cache holding most of the 640 MiB server limit, a GROUP BY of about 160 MiB stays in memory.

CONFIG_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_config.yaml"

> "${CONFIG_FILE}" echo "
max_server_memory_usage: 671088640
page_cache_max_size: 419430400
page_cache_min_size: 0
page_cache_free_memory_ratio: 0
page_cache_policy: LRU
page_cache_history_window_ms: 0
memory_worker_correct_memory_tracker: false
"

server_opts=(
    # we cannot use cgroups since this will sum RSS of all processes and clickhouse-local may fail with MEMORY_LIMIT_EXCEEDED
    --memory_worker_use_cgroup=0
    --memory_worker_dynamic_hard_limit=false
)

$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" --use_page_cache_for_local_disks 1 --local_filesystem_read_method pread --multiquery "
CREATE TABLE t (x UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number FROM numbers(37500000);
OPTIMIZE TABLE t FINAL;
SELECT count() FROM t WHERE NOT ignore(x) FORMAT Null;
SELECT value > 300 * 1024 * 1024 FROM system.metrics WHERE metric = 'PageCacheBytes';
SELECT count() FROM (SELECT number AS k, count() FROM numbers(5000000) GROUP BY k)
SETTINGS max_bytes_ratio_before_external_group_by = 0.5, max_bytes_before_external_group_by = 0, min_bytes_before_external_group_by = 0, max_threads = 2;
SELECT sum(value) FROM system.events WHERE event = 'ExternalAggregationWritePart';
" -- "${server_opts[@]}"

rm "${CONFIG_FILE}"
