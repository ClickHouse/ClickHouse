#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# Named collections are global, so parallel test databases must use different names.
collection="url_archive_custom_method_${CLICKHOUSE_TEST_UNIQUE_NAME}"
function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${collection}" || true
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

-- A table created from the same source is rejected as well, with or without an explicit structure.
CREATE TABLE rejected_${CLICKHOUSE_TEST_UNIQUE_NAME} (x UInt64) AS url(${collection}); -- { serverError BAD_ARGUMENTS }
CREATE TABLE rejected_failover_${CLICKHOUSE_TEST_UNIQUE_NAME} (x UInt64) AS url('http://{127.0.0.1:1|127.0.0.1:2}/missing.zip::entry.csv', 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }

-- An archive read pins one resource, so a failover group is rejected before any request: the metadata
-- probe and the data read would otherwise be free to land on different origins. Both the authority and
-- the path form are rejected.
SELECT * FROM url('http://{127.0.0.1:1|127.0.0.1:2}/missing.zip::entry.csv', 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM urlCluster('test_shard_localhost', 'http://{127.0.0.1:1|127.0.0.1:2}/missing.zip::entry.csv', 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://127.0.0.1:1/{missing|good}.zip::entry.csv', 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:1/{missing|good}.zip::entry.csv', 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }

-- A group in the query or the fragment is a failover option of the same shard, not a separate shard, so the
-- metadata probe and the data read could still land on different origins. Rejected before any request, and a
-- A DESCRIBE with an explicit structure is refused like a SELECT is.
SELECT * FROM url('http://127.0.0.1:1/missing.zip?v={a|b}::entry.csv', 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:1/missing.zip?v={a|b}::entry.csv', 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://127.0.0.1:1/missing.zip?v=a|b::entry.csv', 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://127.0.0.1:1/missing.zip#v={a|b}::entry.csv', 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }
DESCRIBE TABLE url('http://127.0.0.1:1/missing.zip?v={a|b}::entry.csv', 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }

-- With archive syntax disabled, the same URL remains literal and explicit schema needs no HTTP request.
SET allow_archive_path_syntax = 0;
DESCRIBE TABLE url(${collection});
DESCRIBE TABLE urlCluster('test_shard_localhost', ${collection});
"

# The rejection names the cause, not just the code: the same source with a custom method is a plain URL
# read, so a user has to be able to tell this apart from a missing grant or a bad format.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url(${collection}, http_method = 'PUT')" 2>&1 \
    | grep -o 'Archive path syntax is not supported with a custom HTTP method' | head -1
