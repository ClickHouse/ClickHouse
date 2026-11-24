#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3.

# A read of an explicit version of an S3 object (`?versionId=`) does not use the page cache: the metadata of the read,
# and so the ETag that keys the cache, is of the current version of the object, so a read of a historical version
# could get the bytes of the current one, or the other way around. Reads of the current version use the page cache.
#
# The CI server configures no page cache, so everything runs in a private clickhouse-local that has one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_page_cache"
rm -rf "${work_dir}"
mkdir -p "${work_dir}"

> "${work_dir}/config.yaml" echo "
page_cache_max_size: 134217728
query_log:
    database: system
    table: query_log
"

dir="${CLICKHOUSE_TEST_UNIQUE_NAME}"
url="http://localhost:11111/test/${dir}/data.jsonl"

${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --config-file "${work_dir}/config.yaml" --log_queries 1 --query "
    INSERT INTO FUNCTION s3('${url}', 'test', 'testtest', 'JSONEachRow', 's String')
        SELECT repeat('x', number % 100) FROM numbers(1000) SETTINGS s3_truncate_on_insert = 1;
    SELECT count(), sum(length(s)) FROM s3('${url}', 'test', 'testtest', 'JSONEachRow', 's String')
        SETTINGS use_page_cache_for_object_storage = 1, log_comment = '1_current_cold';
    SELECT count(), sum(length(s)) FROM s3('${url}?versionId=null', 'test', 'testtest', 'JSONEachRow', 's String')
        SETTINGS use_page_cache_for_object_storage = 1, log_comment = '2_version';
    SELECT count(), sum(length(s)) FROM s3('${url}?versionId=null', 'test', 'testtest', 'JSONEachRow', 's String')
        SETTINGS use_page_cache_for_object_storage = 1, log_comment = '3_version_again';
    SELECT count(), sum(length(s)) FROM s3('${url}', 'test', 'testtest', 'JSONEachRow', 's String')
        SETTINGS use_page_cache_for_object_storage = 1, log_comment = '4_current_warm';
    SYSTEM FLUSH LOGS query_log;
    SELECT log_comment, ProfileEvents['PageCacheMisses'] > 0, ProfileEvents['PageCacheHits'] > 0 FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND log_comment IN ('1_current_cold', '2_version', '3_version_again', '4_current_warm')
        ORDER BY log_comment;"

rm -rf "${work_dir}"
