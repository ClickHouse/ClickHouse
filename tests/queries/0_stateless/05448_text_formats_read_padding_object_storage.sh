#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: AzureBlobStorage (Azurite) is not available in the fast test.

# Parsing a short CSV object from object storage must not read past the end of the memory that holds it: with a
# direct read, and through the userspace page cache with a direct read, a prefetching read and the reader executor
# (read twice, so that the second read is served from the cache). A sanitizer build aborts on such a read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AZURE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
# Azure container names are limited to 63 characters and must be lowercase alphanumeric, so hash the unique name.
AZURE_CONT="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"

CONFIG_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_page_cache.yaml"
echo "page_cache_max_size: 134217728" > "${CONFIG_FILE}"

# A 14-byte object: "0,0\n1,10\n2,20\n".
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'data.csv', 'CSV', 'auto', 'a UInt32, b UInt32')
    SELECT number, number * 10 FROM numbers(3) SETTINGS azure_truncate_on_insert = 1"

SELECT_QUERY="SELECT * FROM azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'data.csv', 'CSV', 'auto', 'a String, b UInt32')
    ORDER BY b SETTINGS input_format_parallel_parsing = 0"
PAGE_CACHE_USED="SELECT value > 0 FROM system.metrics WHERE metric = 'PageCacheBytes'"
PAGE_CACHE_HIT="SELECT sum(value) > 0 FROM system.events WHERE event = 'PageCacheReadBytes'"

echo "-- direct read"
${CLICKHOUSE_CLIENT} --query "${SELECT_QUERY},
    remote_filesystem_read_method = 'read', use_page_cache_for_object_storage = 0, use_reader_executor = 0"

echo "-- page cache, direct read"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${SELECT_QUERY},
    use_page_cache_for_object_storage = 1, remote_filesystem_read_method = 'read', use_reader_executor = 0;
    ${PAGE_CACHE_USED}"

echo "-- page cache, prefetching read"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${SELECT_QUERY},
    use_page_cache_for_object_storage = 1, remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1,
    use_reader_executor = 0;
    ${PAGE_CACHE_USED}"

echo "-- page cache, reader executor, read twice"
READER_EXECUTOR_QUERY="${SELECT_QUERY},
    use_page_cache_for_object_storage = 1, remote_filesystem_read_method = 'read', use_reader_executor = 1"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${READER_EXECUTOR_QUERY}; ${READER_EXECUTOR_QUERY}; ${PAGE_CACHE_HIT}"

rm "${CONFIG_FILE}"
