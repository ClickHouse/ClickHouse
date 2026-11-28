#!/usr/bin/env bash
# The `buffer_size` / `wait_end_of_query` URL parameters set `http_response_buffer_size` / `http_wait_end_of_query`
# and obey their constraints; the response uses the checked value of a repeated `http_zlib_compression_level`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Not CLICKHOUSE_URL: it carries random values of the constrained settings.
URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?"
CONST_USER="u_const_$CLICKHOUSE_DATABASE"
RO_USER="u_ro_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -m -q "
DROP USER IF EXISTS $CONST_USER, $RO_USER;
CREATE USER $CONST_USER IDENTIFIED WITH plaintext_password BY 'p'
    SETTINGS http_wait_end_of_query = 0 CONST, http_response_buffer_size = 0 CONST, http_zlib_compression_level = 0 CONST;
CREATE USER $RO_USER IDENTIFIED WITH plaintext_password BY 'p' SETTINGS readonly = 1;
"

CONST_URL="${URL}user=$CONST_USER&password=p"
RO_URL="${URL}user=$RO_USER&password=p"
SETTINGS_QUERY="SELECT getSetting('http_wait_end_of_query'), getSetting('http_response_buffer_size')"
BIG_QUERY="SELECT repeat('a', 100000)"

echo '-- the short names are checked against the constraints'
${CLICKHOUSE_CURL} -sS "$CONST_URL&wait_end_of_query=1" -d "SELECT 1" | grep -o -m1 SETTING_CONSTRAINT_VIOLATION
${CLICKHOUSE_CURL} -sS "$CONST_URL&buffer_size=1000000" -d "SELECT 1" | grep -o -m1 SETTING_CONSTRAINT_VIOLATION
${CLICKHOUSE_CURL} -sS "$CONST_URL&wait_end_of_query=0&buffer_size=0" -d "SELECT 1"

echo '-- readonly = 1 can set them, the setting name wins'
${CLICKHOUSE_CURL} -sS "$RO_URL&wait_end_of_query=1&buffer_size=1000" -d "$SETTINGS_QUERY"
${CLICKHOUSE_CURL} -sS "$RO_URL&wait_end_of_query=1&http_wait_end_of_query=0" -d "SELECT getSetting('http_wait_end_of_query')"

echo '-- the last http_zlib_compression_level is used'
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' "${URL}enable_http_compression=1&http_zlib_compression_level=9" -d "$BIG_QUERY" | wc -c | awk '{ print ($1 < 100000) }'
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' "$CONST_URL&enable_http_compression=1&http_zlib_compression_level=9&http_zlib_compression_level=0" -d "$BIG_QUERY" | wc -c | awk '{ print ($1 > 100000) }'

$CLICKHOUSE_CLIENT -q "DROP USER $CONST_USER, $RO_USER"
