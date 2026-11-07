#!/usr/bin/env bash
# Tags: no-fasttest
# The http_method validation errors must not show the URL password, presigned signature or access token.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PW="pw_${CLICKHOUSE_TEST_UNIQUE_NAME}"
SIG="sig_${CLICKHOUSE_TEST_UNIQUE_NAME}"
TOK="tok_${CLICKHOUSE_TEST_UNIQUE_NAME}"
WILD="http://alice:${PW}@localhost:1/files/*.csv?X-Amz-Signature=${SIG}&access_token=${TOK}"

# Prints the error code and whether the password, signature or token leaked (0 = hidden).
# The client's own "(query: ...)" echo repeats the query text, so it is not counted.
check()
{
    local out
    out=$($CLICKHOUSE_CLIENT --server_logs_file=/dev/null "$@" 2>&1)
    echo "$(echo "$out" | grep -o -m1 'BAD_ARGUMENTS') $(echo "$out" | grep -v '^(query: ' | grep -c -e "${PW}" -e "${SIG}" -e "${TOK}")"
}

check -q "SELECT * FROM url('${WILD}', CSV, 'x UInt8', http_method='POST')"
check -q "SELECT * FROM url('${WILD}', CSV, http_method='POST')"
check -q "CREATE TABLE url_mask_${CLICKHOUSE_DATABASE} (x UInt8) ENGINE = URL('${WILD}', CSV, http_method='PUT')"
check --url_base="http://alice:${PW}@localhost:1/" -q "SELECT * FROM url('files/*.csv?X-Amz-Signature=${SIG}&access_token=${TOK}', CSV, 'x UInt8', http_method='POST')"
check -q "SELECT * FROM url('s3://bucket/f.csv?X-Amz-Signature=${SIG}&access_token=${TOK}', CSV, 'x UInt8', http_method='POST')"
check -q "CREATE TABLE url_mask_s3_${CLICKHOUSE_DATABASE} (x UInt8) ENGINE = URL('s3://bucket/f.csv?X-Amz-Signature=${SIG}&access_token=${TOK}', CSV, http_method='POST')"

# Brace and failover patterns hold several authorities, each with its own password.
BRACE="{http://alice:${PW}@localhost:1/a,http://bob:${PW}_b@localhost:1/b}/*.csv"
FAILOVER="http://alice:${PW}@localhost:1/a/*.csv|http://bob:${PW}_b@localhost:1/b/*.csv"
check -q "SELECT * FROM url('${BRACE}', CSV, 'x UInt8', http_method='POST')"
check -q "SELECT * FROM url('${BRACE}', CSV, http_method='POST')"
check -q "CREATE TABLE url_mask_brace_${CLICKHOUSE_DATABASE} (x UInt8) ENGINE = URL('${BRACE}', CSV, http_method='PUT')"
check -q "SELECT * FROM url('${FAILOVER}', CSV, 'x UInt8', http_method='POST')"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0, countIf(position(exception, '${PW}') > 0 OR position(exception, '${SIG}') > 0
        OR position(exception, '${TOK}') > 0)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'ExceptionBeforeStart' AND event_date >= yesterday()
        AND exception LIKE '%http_method%'"
