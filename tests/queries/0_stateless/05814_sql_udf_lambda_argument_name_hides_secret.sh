#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: encrypt requires the OpenSSL-based functions, absent in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A lambda argument of a SQL UDF call is named by its text, which must not bring a secret into the column name.
m="${CLICKHOUSE_DATABASE}_map"

$CLICKHOUSE_CLIENT -q "CREATE FUNCTION $m AS (fn, xs) -> arrayMap(fn, xs)"
$CLICKHOUSE_CLIENT -q "
SELECT $m(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYT')), ['a'])
SETTINGS format_display_secrets_in_show_and_select = 0
FORMAT TSVWithNames" | grep -c 'SEKRIT'
$CLICKHOUSE_CLIENT -q "DROP FUNCTION $m"
