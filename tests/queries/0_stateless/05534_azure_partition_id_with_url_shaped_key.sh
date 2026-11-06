#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on Azure

# A partition key that looks like a fully qualified path, such as `https://example.com/a.tsv`, would make the S3 object
# storage write to another bucket, so it is refused there. Azure writes it as a literal blob name in the configured
# container, so the write must keep working.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AZURE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
AZURE_CONT="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', '{_partition_id}.tsv', 'TSV', 'auto', 'p String, x UInt32')
    PARTITION BY p VALUES ('https://example.com/a', 1)"

$CLICKHOUSE_CLIENT -q "
    SELECT p, x FROM azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'https://example.com/a.tsv', 'TSV', 'auto', 'p String, x UInt32')"
