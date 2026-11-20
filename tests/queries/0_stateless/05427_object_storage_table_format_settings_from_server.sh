#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: S3 (MinIO) and AzureBlobStorage (Azurite) are not available in the fast test.

# An `S3` or `AzureBlobStorage` table takes the format settings of the server plus its `SETTINGS` clause, as `File`
# and `URL` do, not those of the session that creates it: the definition does not keep the session's settings, so the
# table would read differently once it is attached again, e.g. on server startup
# (https://github.com/ClickHouse/ClickHouse/issues/126789).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_URL="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
AZURE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
# Azure container names are limited to 63 characters and must be lowercase alphanumeric, so hash the unique name.
AZURE_CONT="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"

S3_ENGINE="S3('${S3_URL}', 'test', 'testtest', 'CSV')"
AZURE_ENGINE="AzureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'data.csv', 'CSV')"

# A file whose values are separated by ';'.
${CLICKHOUSE_CLIENT} --format_csv_delimiter ';' --query "
    INSERT INTO FUNCTION s3('${S3_URL}', 'test', 'testtest', 'CSV', 'a Int32, b Int32')
    SELECT number, number * 10 FROM numbers(3) SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} --format_csv_delimiter ';' --query "
    INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'data.csv', 'CSV', 'auto', 'a Int32, b Int32')
    SELECT number, number * 10 FROM numbers(3) SETTINGS azure_truncate_on_insert = 1"

for engine in S3 AZURE; do
    var="${engine}_ENGINE"
    definition="${!var}"
    lower=$(echo "${engine}" | tr '[:upper:]' '[:lower:]')

    echo "-- ${engine}: a session setting does not apply, even in the session that creates the table"
    ${CLICKHOUSE_CLIENT} --format_csv_delimiter ';' --query "
        CREATE TABLE ${lower}_session (a Int32, b Int32) ENGINE = ${definition};
        SELECT * FROM ${lower}_session ORDER BY a" 2>&1 | grep -o -m1 'CANNOT_PARSE_INPUT_ASSERTION_FAILED'

    echo "-- ${engine}: the SETTINGS clause applies, in any session and after the table is attached again"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${lower}_stated (a Int32, b Int32) ENGINE = ${definition} SETTINGS format_csv_delimiter = ';';
        SELECT * FROM ${lower}_stated ORDER BY a;
        DETACH TABLE ${lower}_stated;
        ATTACH TABLE ${lower}_stated;
        SELECT * FROM ${lower}_stated ORDER BY a;
        DROP TABLE ${lower}_session;
        DROP TABLE ${lower}_stated;"
done
