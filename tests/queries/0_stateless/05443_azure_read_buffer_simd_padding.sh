#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads from Azurite

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A reader that owns its buffer sizes it to the object when the object is smaller than the buffer,
# and the CSV parser reads up to 15 bytes past the bytes it consumes. Without the SIMD padding past
# the end of that buffer this is a heap-buffer-overflow, which ASan reports.

AZURE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
# Azure container names are limited to 63 characters and must be lowercase alphanumeric, so hash
# the unique name instead of embedding it.
AZURE_CONT="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"

TABLE_FN="azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'data.csv', 'CSV', 'auto', 's String')"

# 14 bytes on the wire, far below any buffer size.
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION ${TABLE_FN} SELECT 'hello world' SETTINGS azure_truncate_on_insert = 1"

# `read` makes the reader hand out its own buffer, instead of the padded buffer of the prefetching wrapper.
${CLICKHOUSE_CLIENT} --remote_filesystem_read_method=read --query "SELECT * FROM ${TABLE_FN}"
