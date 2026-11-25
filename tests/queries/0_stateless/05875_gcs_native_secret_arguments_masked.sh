#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `gcs` table function needs the S3 support, which the fast-test build does not have.
#
# The native GCS credentials of the `gcs` table function, `access_token` and `service_account_key`, must be masked
# in the logged query even when the query is rejected: the query is logged before its arguments are validated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The markers are split here, so that only the queries under test contain them as a whole.
TOKEN="token-secret-${CLICKHOUSE_DATABASE}"
KEY="private-key-secret-${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --use_native_gcs=1 -q "
    SELECT * FROM gcs('https://storage.googleapis.com/bucket/data', NOSIGN, 'TSV', 'x UInt8', access_token = '${TOKEN}')" >/dev/null 2>&1
$CLICKHOUSE_CLIENT --use_native_gcs=1 -q "
    SELECT * FROM gcs('https://storage.googleapis.com/bucket/data', NOSIGN, 'TSV', 'x UInt8', service_account_key = '{\"private_key\": \"${KEY}\"}')" >/dev/null 2>&1

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0, countIf(position(query, '${TOKEN:0:12}' || '${TOKEN:12}') > 0 OR position(query, '${KEY:0:12}' || '${KEY:12}') > 0)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND position(query, 'gcs(') > 0
        AND query NOT LIKE '%system.query_log%'"
