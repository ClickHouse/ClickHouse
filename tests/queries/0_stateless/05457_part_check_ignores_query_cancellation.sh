#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-object-storage, no-parallel-replicas, zookeeper, no-shared-merge-tree
# no-parallel: enables a global pauseable failpoint which pauses every filesystem cache write on the server.
# zookeeper: a part check is requested through the broken-part callback, which only `ReplicatedMergeTree` installs.
# no-shared-merge-tree: the table pins a private cache disk, and SharedMergeTree requires a keeper-metadata disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query that waits in `FileSegment::wait` for another query's cache download, and is cancelled while it
# waits, throws `TIMEOUT_EXCEEDED` or `QUERY_WAS_CANCELLED` from inside `MergeTreeReaderWide::readRows`.
# Neither code is a retryable exception, so the reader reported the part as broken and a healthy part was
# enqueued for a full data check. On object storage such a check reads the whole part, and a replica
# restart (`partialShutdown` -> `ReplicatedMergeTreePartCheckThread::stop`) blocks until it ends.
#
# Here the cancellation comes from `max_execution_time`: the cancellation checker kills the query while
# its only thread waits for the paused download, and the wait loop observes the kill and throws.
# A killed or timed-out query says nothing about the data, so no check must be requested.

# A private cache keeps the test independent from other cache users on the server. The whole data file
# must fit in one file segment, so that the waiter below never becomes a downloader itself (it would
# pause on its own cache write, because the failpoint is disabled only after it returns).
cache_name="cache_05457_${CLICKHOUSE_DATABASE}"
table="t_part_check_cancel"

# The check thread logs under the table name, which a repeated run in the same database shares with
# the earlier runs, so the log query below is bounded to this run.
started_at=$($CLICKHOUSE_CLIENT --query "SELECT toString(now64(6))")

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $table SYNC;
    CREATE TABLE $table (a UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$table', 'r1')
    ORDER BY a
    SETTINGS
        min_bytes_for_wide_part = 0,
        disk = disk(
            type = cache,
            name = '$cache_name',
            path = '$cache_name/',
            max_size = '1Gi',
            max_file_segment_size = '256Mi',
            cache_on_write_operations = 0,
            load_metadata_asynchronously = 0,
            disk = 'local_disk');
    INSERT INTO $table SELECT number FROM numbers(1000000);
"

# Warm up the in-memory mark cache, so that both queries below take marks from it and their only
# filesystem cache accesses are for the data file. Otherwise the paused downloader would hold the
# mark cache load token and the waiter would block on it instead of `FileSegment::wait`.
# Strictly synchronous, no prefetches and no background download: a cache write still in flight
# after this query would pause on the failpoint while holding a segment the queries below need.
$CLICKHOUSE_CLIENT --max_threads 1 \
    --remote_filesystem_read_prefetch 0 \
    --allow_prefetched_read_pool_for_remote_filesystem 0 \
    --filesystem_cache_allow_background_download 0 \
    --query "SELECT a FROM $table LIMIT 1 FORMAT Null"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DROP FILESYSTEM CACHE '$cache_name';
    SYSTEM ENABLE FAILPOINT file_segment_pause_before_write;
"

# The failpoint is server-global, so it must be disabled even if the script aborts before reaching
# the explicit disable below. Otherwise every filesystem cache write on the server stays paused and
# the following tests hang behind it, masking the failure that happened here.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT file_segment_pause_before_write" > /dev/null 2>&1 || true' EXIT

# The downloader pauses inside its first cache write, holding the file segment in DOWNLOADING state.
$CLICKHOUSE_CLIENT --max_threads 1 \
    --enable_filesystem_cache 1 \
    --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0 \
    --remote_filesystem_read_prefetch 0 \
    --allow_prefetched_read_pool_for_remote_filesystem 0 \
    --filesystem_cache_allow_background_download 0 \
    --query "SELECT sum(a) FROM $table" &
downloader_pid=$!

# The failpoint pauses every cache write on the server, so the first pause is not necessarily the
# downloader's: additionally wait until the downloader holds the segment of this test's cache.
# Bounded, so that a downloader that never writes to the cache is reported instead of blocking
# the test until its timeout.
if ! timeout 120 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT file_segment_pause_before_write PAUSE" > /dev/null 2>&1; then
    echo "FAIL: no cache write paused on the failpoint"
fi
downloading=0
for _ in {1..600}; do
    downloading=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.filesystem_cache WHERE cache_name = '$cache_name' AND state = 'DOWNLOADING'")
    [[ "$downloading" == "1" ]] && break
    sleep 0.1
done
echo "$downloading"

# The waiter needs the paused segment. Its cache wait is long enough to outlive `max_execution_time`,
# so the cancellation checker kills it while it waits, and the wait loop throws `TIMEOUT_EXCEEDED`
# from inside the read. `Cache info:` in the error proves the throw happened inside the cache read
# buffer, i.e. under the reader's catch, and not at a cancellation point of the pipeline.
# Backgrounded with a bounded wait, so that a waiter which unexpectedly pauses on the failpoint
# itself is reported instead of blocking the test until its timeout.
waiter_out="${CLICKHOUSE_TMP:-/tmp}/05457_waiter_${CLICKHOUSE_DATABASE}.out"
rm -f "$waiter_out"
$CLICKHOUSE_CLIENT --max_threads 1 \
    --enable_filesystem_cache 1 \
    --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0 \
    --remote_filesystem_read_prefetch 0 \
    --allow_prefetched_read_pool_for_remote_filesystem 0 \
    --filesystem_cache_allow_background_download 0 \
    --use_uncompressed_cache 0 \
    --filesystem_cache_wait_for_concurrent_download_timeout_milliseconds 600000 \
    --max_execution_time 3 \
    --query "SELECT sum(a) FROM $table" > "$waiter_out" 2>&1 &
waiter_pid=$!

waiter_finished=0
for _ in {1..600}; do
    if ! kill -0 "$waiter_pid" 2>/dev/null; then
        waiter_finished=1
        break
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT file_segment_pause_before_write"
wait "$downloader_pid"
wait "$waiter_pid" 2>/dev/null || true

waiter_result=$(cat "$waiter_out")
rm -f "$waiter_out"
if [[ "$waiter_finished" != "1" ]]; then
    echo "FAIL: the waiter did not finish while the failpoint was armed: $waiter_result"
elif [[ "$waiter_result" == *"Timeout exceeded"*"Cache info:"* ]]; then
    echo "waiter timed out inside the cache read"
else
    echo "FAIL: unexpected waiter result: $waiter_result"
fi

# The part is healthy, so a requested check ends with "looks good": the observable harm is that a
# check was requested at all. The check thread logs `Checking part` when it picks the part up, which
# is shortly after the request, so poll for a bounded time. With the fix the loop runs to its bound.
checks=0
for _ in {1..30}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    checks=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.text_log
        WHERE logger_name LIKE concat(currentDatabase(), '.$table%PartCheckThread%')
          AND message LIKE 'Checking part %'
          AND event_time_microseconds >= toDateTime64('$started_at', 6)")
    [[ "$checks" != "0" ]] && break
    sleep 0.1
done
echo "spurious part checks: $checks"

$CLICKHOUSE_CLIENT --query "DROP TABLE $table SYNC"
