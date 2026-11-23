#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3 (MinIO).

# The page cache key of an object storage read includes the storage (endpoint, bucket), not only the path
# relative to it: a strong ETag identifies only the versions of one object, so the same path with the same
# ETag on another endpoint or in another bucket can have different contents. The same object is read through
# two spellings of the endpoint (`localhost` and `127.0.0.1`): same path, size, and ETag, but a different
# storage, so the read through the second endpoint must not be served from the page cache.
#
# The format is `JSONEachRow` with strings to the end of the file: the reader must not read past the end of a
# page cache cell, which has no padding (checked by sanitizers).
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

file="${CLICKHOUSE_TEST_UNIQUE_NAME}.jsonl"
url_localhost="http://localhost:11111/test/${file}"
url_127="http://127.0.0.1:11111/test/${file}"

${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --config-file "${work_dir}/config.yaml" --log_queries 1 --query "
    INSERT INTO FUNCTION s3('${url_localhost}', 'test', 'testtest', 'JSONEachRow', 's String')
        SELECT repeat('x', number % 100) FROM numbers(1000) SETTINGS s3_truncate_on_insert = 1;
    SELECT count(), sum(length(s)) FROM s3('${url_localhost}', 'test', 'testtest', 'JSONEachRow', 's String')
        SETTINGS use_page_cache_for_object_storage = 1, log_comment = '1_cold';
    SELECT count(), sum(length(s)) FROM s3('${url_localhost}', 'test', 'testtest', 'JSONEachRow', 's String')
        SETTINGS use_page_cache_for_object_storage = 1, log_comment = '2_warm';
    SELECT count(), sum(length(s)) FROM s3('${url_127}', 'test', 'testtest', 'JSONEachRow', 's String')
        SETTINGS use_page_cache_for_object_storage = 1, log_comment = '3_other_endpoint';
    SYSTEM FLUSH LOGS query_log;
    SELECT log_comment, ProfileEvents['PageCacheMisses'] > 0, ProfileEvents['PageCacheHits'] > 0 FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('1_cold', '2_warm', '3_other_endpoint')
        ORDER BY log_comment;"

rm -rf "${work_dir}"
