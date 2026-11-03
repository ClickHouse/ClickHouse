#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the second part reads from S3 (minio)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `filesystem_cache_boundary_alignment` larger than `max_file_segment_size` of the cache
# must not break reads through the cache.

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
CONFIG_FILE="${WORK_DIR}/config.yaml"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"

cat > "${CONFIG_FILE}" <<EOF
filesystem_caches_path: '${WORK_DIR}/caches/'
storage_configuration:
    disks:
        blob:
            type: object_storage
            object_storage_type: local_blob_storage
            metadata_type: plain_rewritable
            path: '${WORK_DIR}/obj/'
        blob_cache:
            type: cache
            disk: blob
            path: '${WORK_DIR}/caches/blob_cache/'
            max_size: '1Gi'
            max_file_segment_size: '1Mi'
            boundary_alignment: '1Mi'
            cache_on_write_operations: 0
            load_metadata_asynchronously: 0
filesystem_caches:
    tiny_cache:
        path: '${WORK_DIR}/caches/tiny_cache/'
        cache_policy: LRU
        max_size: 10
        max_file_segment_size: 100
        boundary_alignment: 100
EOF

function run_local()
{
    $CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" --path "${WORK_DIR}/data" -m -q "$1" 2>&1
}

# Disk read: the cache is empty and the granule is near the end of a ~3 MB column file.
run_local "
CREATE TABLE t (a Int32, b String) ENGINE = MergeTree ORDER BY a SETTINGS disk = 'blob_cache';
INSERT INTO t SELECT number, randomString(100) FROM numbers(30000);
"
run_local "
SYSTEM DROP FILESYSTEM CACHE 'blob_cache';
SELECT count() FROM t WHERE a = 29000 AND length(b) = 100 SETTINGS filesystem_cache_boundary_alignment = 4194304;
SELECT count() > 0 FROM system.filesystem_cache WHERE cache_name = 'blob_cache' AND file_segment_range_begin >= 1048576;
SELECT count() FROM system.filesystem_cache WHERE cache_name = 'blob_cache' AND file_segment_range_begin % 1048576 != 0;
"

# Object storage read through a named cache which keeps no file segment, so each batch starts after an uncached prefix.
FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.bin"
run_local "INSERT INTO FUNCTION s3('http://localhost:11111/test/${FILE}', 'test', 'testtest', 'RawBLOB') SELECT repeat('a', 10000) SETTINGS s3_truncate_on_insert = 1"
run_local "
SELECT length(raw_blob) FROM s3('http://localhost:11111/test/${FILE}', 'test', 'testtest', 'RawBLOB')
SETTINGS filesystem_cache_name = 'tiny_cache', enable_filesystem_cache = 1,
    filesystem_cache_segments_batch_size = 1, filesystem_cache_boundary_alignment = 1048576
"

rm -rf "${WORK_DIR}"
