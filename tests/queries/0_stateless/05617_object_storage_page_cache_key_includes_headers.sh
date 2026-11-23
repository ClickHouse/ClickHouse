#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3.

# The page cache key of an S3 read includes the headers of the read (e.g. `Authorization`, or a key of `SSE-C`), which
# can authenticate it outside of the AWS credentials: the page cache serves bytes without a request, so a reader without
# these headers must not get the bytes read with them. The same object is read with the same credentials with a header,
# then without it, and then with it again: only the last read is served from the page cache.
# A wildcard is used, so that the metadata comes from a listing, without a `HEAD` request.
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
url_wildcard="http://localhost:11111/test/${dir}/*.jsonl"

${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --config-file "${work_dir}/config.yaml" --log_queries 1 --query "
    INSERT INTO FUNCTION s3('${url}', 'test', 'testtest', 'JSONEachRow', 's String')
        SELECT repeat('x', number % 100) FROM numbers(1000) SETTINGS s3_truncate_on_insert = 1;
    SELECT count(), sum(length(s)) FROM s3('${url_wildcard}', 'test', 'testtest', 'JSONEachRow', 's String', headers('X-Test-Identity' = 'a'))
        SETTINGS use_page_cache_for_object_storage = 1, log_comment = '1_cold';
    SELECT count(), sum(length(s)) FROM s3('${url_wildcard}', 'test', 'testtest', 'JSONEachRow', 's String')
        SETTINGS use_page_cache_for_object_storage = 1, log_comment = '2_other_headers';
    SELECT count(), sum(length(s)) FROM s3('${url_wildcard}', 'test', 'testtest', 'JSONEachRow', 's String', headers('X-Test-Identity' = 'a'))
        SETTINGS use_page_cache_for_object_storage = 1, log_comment = '3_warm';
    SYSTEM FLUSH LOGS query_log;
    SELECT log_comment, ProfileEvents['PageCacheMisses'] > 0, ProfileEvents['PageCacheHits'] > 0 FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('1_cold', '2_other_headers', '3_warm')
        ORDER BY log_comment;"

rm -rf "${work_dir}"
