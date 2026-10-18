#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# HTTP media types are case-insensitive: an `application/x-www-form-urlencoded` body binds handler parameters
# regardless of the casing of the `Content-Type` header, and an unframed one is rejected with `411`.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

DB="${CLICKHOUSE_DATABASE}"
P="/h_${DB}"
HID="hcase_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$HID\`"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HID\` URL '${P}/id' METHODS (POST, PUT) AS SELECT {id:UInt64} * 2 AS r FORMAT TSV"

echo "=== lowercase ==="
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'param_id=21' "${BASE}${P}/id"
echo "=== mixed case ==="
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: Application/X-WWW-Form-Urlencoded' --data-binary 'param_id=21' "${BASE}${P}/id"
${CLICKHOUSE_CURL} -sS -X PUT -H 'Content-Type: APPLICATION/X-WWW-FORM-URLENCODED; charset=utf-8' --data-binary 'param_id=3' "${BASE}${P}/id"
echo "=== mixed case without framing is rejected ==="
${CLICKHOUSE_CURL} -sS -X PUT -H 'Content-Type: Application/X-WWW-Form-Urlencoded' -o /dev/null -w '%{http_code}\n' "${BASE}${P}/id"
