#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs MinIO

# The server config gives this endpoint a server-side encryption key (`s3_settings_override.xml`). A restricted session
# drops it when it creates the table, and a later query must not merge it back. MinIO refuses such a key over http
# with code 400; without it the request is anonymous, which the bucket may allow or refuse with code 403.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_05628_${CLICKHOUSE_DATABASE}"
trap '$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"' EXIT

$CLICKHOUSE_CLIENT --s3_allow_server_credentials_in_user_queries 0 -q "
    CREATE TABLE ${TABLE} (x UInt8)
    ENGINE = S3('http://localhost:11111/test/05628_s3_restricted_client_keeps_filtered_auth/missing.tsv', 'TSV')
"

result=$($CLICKHOUSE_CLIENT --s3_allow_server_credentials_in_user_queries 0 --s3_ignore_file_doesnt_exist 1 \
    -q "SELECT count() FROM ${TABLE}" 2>&1)
if [[ "$result" == "0" || "$result" == *"HTTP response code: 403"* ]]; then
    echo "ok"
else
    echo "$result"
fi
