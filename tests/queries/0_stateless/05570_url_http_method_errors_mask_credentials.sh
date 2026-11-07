#!/usr/bin/env bash
# Tags: no-fasttest
# The http_method validation errors must not show the URL password or presigned signature.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PW="pw_${CLICKHOUSE_TEST_UNIQUE_NAME}"
SIG="sig_${CLICKHOUSE_TEST_UNIQUE_NAME}"
WILD="http://alice:${PW}@localhost:1/files/*.csv?X-Amz-Signature=${SIG}"

# Prints the error code and whether the password or the signature leaked (0 = hidden).
# The client's own "(query: ...)" echo repeats the query text, so it is not counted.
check()
{
    local out
    out=$($CLICKHOUSE_CLIENT --server_logs_file=/dev/null "$@" 2>&1)
    echo "$(echo "$out" | grep -o -m1 'BAD_ARGUMENTS') $(echo "$out" | grep -v '^(query: ' | grep -c -e "${PW}" -e "${SIG}")"
}

check -q "SELECT * FROM url('${WILD}', CSV, 'x UInt8', http_method='POST')"
check -q "SELECT * FROM url('${WILD}', CSV, http_method='POST')"
check -q "CREATE TABLE url_mask_${CLICKHOUSE_DATABASE} (x UInt8) ENGINE = URL('${WILD}', CSV, http_method='PUT')"
check --url_base="http://alice:${PW}@localhost:1/" -q "SELECT * FROM url('files/*.csv?X-Amz-Signature=${SIG}', CSV, 'x UInt8', http_method='POST')"
check -q "SELECT * FROM url('s3://bucket/f.csv?X-Amz-Signature=${SIG}', CSV, 'x UInt8', http_method='POST')"
check -q "CREATE TABLE url_mask_s3_${CLICKHOUSE_DATABASE} (x UInt8) ENGINE = URL('s3://bucket/f.csv?X-Amz-Signature=${SIG}', CSV, http_method='POST')"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0, countIf(position(exception, '${PW}') > 0 OR position(exception, '${SIG}') > 0)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'ExceptionBeforeStart' AND event_date >= yesterday()
        AND exception LIKE '%http_method%'"
