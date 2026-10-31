#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A SQL UDF used in `APPLY` is replaced by its body. The name of the call (`f(a)`) still contains the column name,
# so the collision of a terminal call requires a subquery alias, but a following transformer reads the name of the body,
# which does not depend on the column when the body ignores its argument or names its result with an alias.
# UDFs are global, so their names include the database name to keep concurrent runs of the test apart.

f_constant="f_constant_${CLICKHOUSE_DATABASE}"
f_aliased="f_aliased_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE FUNCTION ${f_constant} AS (y) -> 0;
    CREATE FUNCTION ${f_aliased} AS (y) -> (y + 1 AS z);
"

$CLICKHOUSE_CLIENT -q "SELECT * APPLY ${f_constant} FROM (SELECT 1 AS a), (SELECT 2 AS a)" 2>&1 | grep -o -m1 'ALIAS_REQUIRED'
$CLICKHOUSE_CLIENT -q "SELECT * APPLY ${f_constant} APPLY toString FROM (SELECT 1 AS a), (SELECT 2 AS a) FORMAT TSVWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * APPLY ${f_aliased} APPLY toString FROM (SELECT 1 AS a), (SELECT 2 AS a) FORMAT TSVWithNames"

$CLICKHOUSE_CLIENT -q "
    DROP FUNCTION ${f_constant};
    DROP FUNCTION ${f_aliased};
"
