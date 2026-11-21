#!/usr/bin/env bash
# Tags: no-distributed-cache
# Filesystem cache under a non-ASCII cache directory: segments must be created,
# found again after a restart and physically removed on eviction.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR:?}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR:?}"' EXIT

cat > "${WORK_DIR}/config.xml" <<CONFIG
<clickhouse>
    <storage_configuration>
        <disks>
            <blob>
                <type>local_blob_storage</type>
                <path>${WORK_DIR}/blob/</path>
            </blob>
            <blob_cache>
                <type>cache</type>
                <disk>blob</disk>
                <path>кэш_キャッシュ/</path>
                <max_size>100Mi</max_size>
                <max_file_segment_size>4Mi</max_file_segment_size>
                <background_download_threads>0</background_download_threads>
            </blob_cache>
        </disks>
        <policies>
            <cached>
                <volumes>
                    <main>
                        <disk>blob_cache</disk>
                    </main>
                </volumes>
            </cached>
        </policies>
    </storage_configuration>
</clickhouse>
CONFIG

function run_local()
{
    $CLICKHOUSE_LOCAL --config-file "${WORK_DIR}/config.xml" --path "${WORK_DIR}/data" -m -q "$1"
}

function count_cache_files()
{
    find "${WORK_DIR}/data" -path '*кэш_キャッシュ*' -type f | wc -l
}

run_local "
    CREATE DATABASE db ENGINE = Atomic;
    CREATE TABLE db.test (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'cached';
    INSERT INTO db.test SELECT number, toString(number) FROM numbers(1000);
    SYSTEM DROP FILESYSTEM CACHE 'blob_cache';
    -- sum(length(b)) makes the query read a column: sum(a), count() alone can be answered without reading any data.
    SELECT sum(a), count(), sum(length(b)) FROM db.test;
    SELECT count() > 0, sum(downloaded_size) > 0, countIf(cache_path LIKE '%/кэш_キャッシュ/%') = count() FROM system.filesystem_cache;
    -- Do not load the table in the next run: loading its parts reads through the cache.
    DETACH TABLE db.test PERMANENTLY;
"
[ "$(count_cache_files)" -gt 0 ] && echo "files created"

# After a restart the metadata is loaded from the same directory and eviction removes the files.
run_local "
    SELECT count() > 0 FROM system.filesystem_cache;
    SYSTEM DROP FILESYSTEM CACHE 'blob_cache';
    SELECT count() FROM system.filesystem_cache;
"
count_cache_files
