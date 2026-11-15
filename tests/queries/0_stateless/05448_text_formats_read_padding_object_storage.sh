#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: S3 (MinIO) is not available in the fast test.

# Parsing a short CSV object through the userspace page cache with a prefetching read must not read past the end of the
# page cache cell that holds it. A sanitizer build aborts on such a read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_URL="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/data.csv"

CONFIG_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_page_cache.yaml"
echo "page_cache_max_size: 134217728" > "${CONFIG_FILE}"

# A 14-byte object: "0,0\n1,10\n2,20\n".
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${S3_URL}', 'test', 'testtest', 'CSV', 'a UInt32, b UInt32')
    SELECT number, number * 10 FROM numbers(3) SETTINGS s3_truncate_on_insert = 1"

echo "-- page cache, prefetching read"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    SELECT * FROM s3('${S3_URL}', 'test', 'testtest', 'CSV', 'a String, b UInt32') ORDER BY b
    SETTINGS input_format_parallel_parsing = 0, use_page_cache_for_object_storage = 1,
    remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1;
    SELECT value > 0 FROM system.metrics WHERE metric = 'PageCacheBytes'"

rm "${CONFIG_FILE}"
