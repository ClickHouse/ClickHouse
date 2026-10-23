#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# Named collections are global, so parallel test databases must use different names.
collection="url_archive_custom_method_${CLICKHOUSE_TEST_UNIQUE_NAME}"
function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${collection}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE NAMED COLLECTION ${collection} AS
    url = 'http://127.0.0.1:1/missing.zip::entry.csv',
    format = 'CSV', structure = 'x UInt64', method = 'POST';

-- Reject archive syntax before either execution or schema inference can make HTTP requests.
SET allow_archive_path_syntax = 1;
SELECT * FROM url(${collection}); -- { serverError BAD_ARGUMENTS }
SELECT * FROM urlCluster('test_shard_localhost', ${collection}); -- { serverError BAD_ARGUMENTS }
DESCRIBE TABLE url(${collection}, structure = 'auto'); -- { serverError BAD_ARGUMENTS }
DESCRIBE TABLE urlCluster('test_shard_localhost', ${collection}, structure = 'auto'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url(${collection}, http_method = 'PUT'); -- { serverError BAD_ARGUMENTS }

-- With archive syntax disabled, the same URL remains literal and explicit schema needs no HTTP request.
SET allow_archive_path_syntax = 0;
DESCRIBE TABLE url(${collection});
DESCRIBE TABLE urlCluster('test_shard_localhost', ${collection});
"
