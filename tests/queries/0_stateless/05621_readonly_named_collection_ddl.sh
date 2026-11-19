#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Named collection DDL is rejected in readonly mode and with allow_ddl = 0, like other DDL.

nc="nc_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${nc}_existing"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${nc}_new"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${nc}_existing AS key = 'value'"

for setting in "--readonly 1" "--readonly 2" "--allow_ddl 0"; do
    echo "$setting"
    $CLICKHOUSE_CLIENT $setting -q "CREATE NAMED COLLECTION ${nc}_new AS key = 'value'" 2>&1 | grep -o -m1 -E 'READONLY|QUERY_IS_PROHIBITED'
    $CLICKHOUSE_CLIENT $setting -q "ALTER NAMED COLLECTION ${nc}_existing SET key = 'other'" 2>&1 | grep -o -m1 -E 'READONLY|QUERY_IS_PROHIBITED'
    $CLICKHOUSE_CLIENT $setting -q "DROP NAMED COLLECTION ${nc}_existing" 2>&1 | grep -o -m1 -E 'READONLY|QUERY_IS_PROHIBITED'
done

# Nothing was created or dropped.
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.named_collections WHERE name LIKE '${nc}%' ORDER BY name" | sed "s/${nc}/nc/"

$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${nc}_existing"
