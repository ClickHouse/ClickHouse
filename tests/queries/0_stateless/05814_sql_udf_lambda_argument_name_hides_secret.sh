#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: encrypt requires the OpenSSL-based functions, absent in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A lambda argument of a SQL UDF call is named by its text: a secret in it is masked, and two different lambdas still
# get different names.
m="${CLICKHOUSE_DATABASE}_map"
um="${CLICKHOUSE_DATABASE}_untuple_map"

$CLICKHOUSE_CLIENT -q "
CREATE FUNCTION $m AS (fn, xs) -> arrayMap(fn, xs);
CREATE FUNCTION $um AS (fn, xs) -> untuple(tuple(arrayMap(fn, xs))) "

res=$($CLICKHOUSE_CLIENT --format_display_secrets_in_show_and_select 0 -q "
SELECT $m(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYT')), ['a']), $m(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYZ')), ['a'])
FORMAT TSVWithNames;
SELECT * FROM (SELECT $m(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYT')), ['a']), $m(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYZ')), ['a']))
FORMAT TSVWithNames;
SELECT * FROM (SELECT $um(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYT')), ['a']), $um(x -> hex(encrypt('aes-128-ecb', x, 'SEKRIT_KEY_16BYZ')), ['a']))
FORMAT TSVWithNames;" 2>&1)
echo "$res" | sed "s/${CLICKHOUSE_DATABASE}_//g"
echo "$res" | grep -c 'SEKRIT'

$CLICKHOUSE_CLIENT -q "
DROP FUNCTION $m;
DROP FUNCTION $um"
