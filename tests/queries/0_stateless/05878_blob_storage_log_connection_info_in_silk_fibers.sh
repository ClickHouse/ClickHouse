#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3

# The `system.blob_storage_log` rows of a native multipart copy that runs in Silk fibers name their connection, as they
# do for a copy in threads. The parts are copied in parallel, so fibers interleave on scheduler threads and resume on
# other threads in the middle of a request; the state that pairs a row with its connection follows the fiber.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS data;
    DROP TABLE IF EXISTS data_restored;
    CREATE TABLE data (key UInt64, s String) ENGINE = MergeTree ORDER BY key
        SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
    INSERT INTO data SELECT number, randomString(1024) FROM numbers(32768);
    OPTIMIZE TABLE data FINAL;
"

prefix="backups/$CLICKHOUSE_DATABASE"
# The ~32 MiB column file exceeds the threshold, so it is copied with several parallel UploadPartCopy requests.
silk_copy_settings=(
    --backup_restore_s3_use_silk 1
    --s3_max_single_operation_copy_size 5242880
    --s3_min_upload_part_size 5242880
)

$CLICKHOUSE_CLIENT --format Null "${silk_copy_settings[@]}" --query_id "${CLICKHOUSE_DATABASE}_backup" \
    -q "BACKUP TABLE data TO S3(s3_conn, '$prefix/fibers') SETTINGS allow_s3_native_copy = 1"
$CLICKHOUSE_CLIENT --format Null "${silk_copy_settings[@]}" --query_id "${CLICKHOUSE_DATABASE}_restore" \
    -q "RESTORE TABLE data AS data_restored FROM S3(s3_conn, '$prefix/fibers') SETTINGS allow_s3_native_copy = 1"

$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS blob_storage_log, query_log;

    -- The copies ran in fibers.
    SELECT
        sumIf(ProfileEvents['S3MultipartCopyInFibers'], query_id = '${CLICKHOUSE_DATABASE}_backup') > 0,
        sumIf(ProfileEvents['S3MultipartCopyInFibers'], query_id = '${CLICKHOUSE_DATABASE}_restore') > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
        AND type = 'QueryFinish' AND query_id IN ('${CLICKHOUSE_DATABASE}_backup', '${CLICKHOUSE_DATABASE}_restore');

    -- Every successful row of a multipart copy names its connection.
    SELECT
        query_id = '${CLICKHOUSE_DATABASE}_backup' ? 'backup' : 'restore' AS query,
        countIf(event_type = 'Copy') >= 4,
        countIf(connection_id = 0)
    FROM system.blob_storage_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND query_id IN ('${CLICKHOUSE_DATABASE}_backup', '${CLICKHOUSE_DATABASE}_restore')
        AND event_type IN ('MultiPartUploadCreate', 'Copy', 'MultiPartUploadComplete') AND error_code = 0
    GROUP BY query
    ORDER BY query;

    DROP TABLE data;
    DROP TABLE data_restored;
"
