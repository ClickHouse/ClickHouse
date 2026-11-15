#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The request body of a SQL-defined handler is appended to the handler's query only as the data of an `INSERT`.
# It must never extend the SQL of any other query: neither by passing `_request_body` in the URL (which used to
# leave the body unread, so it was appended to the query text), nor when `_request_body` is bound otherwise.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="hrb_${DB}"
P="/hrb_${DB}"

cleanup() {
    for suffix in welcome capture capture_get insert input input_no_format; do
        $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_${suffix}\`"
    done
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_welcome\` URL '${P}/welcome' METHODS (POST) AS SELECT concat('Welcome, ', {_request_body:String}) AS message"

echo "=== _request_body is bound from the body ==="
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/welcome" --data-binary 'Alice'

echo "=== _request_body cannot be passed in the URL ==="
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/welcome?param__request_body=Alice" -H 'Content-Type: text/plain' --data-binary 'UNION ALL SELECT 1+1' | grep -o -m1 "BAD_ARGUMENTS"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/welcome?_request_body=Alice" -H 'Content-Type: text/plain' --data-binary 'UNION ALL SELECT 1+1' | grep -o -m1 "BAD_ARGUMENTS"

echo "=== _request_body bound from a URL capture group: the body is not appended to the query ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_capture\` URL REGEXP '${P}/capture/(?P<_request_body>[a-z]+)' METHODS (POST) AS SELECT concat('Welcome, ', {_request_body:String}) AS message"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/capture/bob" -H 'Content-Type: text/plain' --data-binary 'UNION ALL SELECT 1+1'

echo "=== _request_body bound from a URL capture group does not read the body: GET is allowed, no Content-Length needed ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_capture_get\` URL REGEXP '${P}/capture_get/(?P<_request_body>[a-z]+)' AS SELECT concat('Welcome, ', {_request_body:String}) AS message"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/capture_get/carol"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/capture/dave" -H 'Transfer-Encoding:' -H 'Content-Length:'

echo "=== _request_body bound from a URL capture group cannot be passed in the URL query string either ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/capture_get/erin?param__request_body=Eve" | grep -o -m1 "BAD_ARGUMENTS"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/capture_get/erin?_request_body=Eve" | grep -o -m1 "BAD_ARGUMENTS"

echo "=== the body is still the data of an INSERT ==="
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t05407 (s String) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_insert\` URL '${P}/insert' METHODS (POST) AS INSERT INTO ${DB}.t05407 FORMAT TSV"
printf 'hello\nworld\n' | ${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/insert" --data-binary @-
$CLICKHOUSE_CLIENT -q "SELECT s FROM ${DB}.t05407 ORDER BY s"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.t05407"

echo "=== the body is the data of an INSERT ... SELECT from input ==="
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t05407_input (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_input\` URL '${P}/input' METHODS (POST) AS INSERT INTO ${DB}.t05407_input SELECT x FROM input('x UInt64') FORMAT TSV"
printf '1\n2\n' | ${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/input" --data-binary @-
$CLICKHOUSE_CLIENT -q "SELECT x FROM ${DB}.t05407_input ORDER BY x"

echo "=== the body is never parsed as SQL, even without a literal FORMAT in the query ==="
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE ${DB}.t05407_input"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_input_no_format\` URL '${P}/input_no_format' METHODS (POST) AS INSERT INTO ${DB}.t05407_input SELECT x FROM input('x UInt64')"
printf 'UNION ALL SELECT 99 FORMAT TSV\n1\n' | ${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/input_no_format" --data-binary @- | grep -o -m1 "UNKNOWN_FORMAT"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${DB}.t05407_input"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.t05407_input"

echo "=== _request_body left in a named session is not used: it is bound from the current body ==="
SESSION="session_05407_${DB}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION}" --data-binary "SET param__request_body = 'Stale'"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/welcome?session_id=${SESSION}" -H 'Content-Type: text/plain' --data-binary 'Alice'
