#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="user_05489_${CLICKHOUSE_DATABASE}"
table_name="t_05489_${CLICKHOUSE_DATABASE}"
dict_name="d_05489_${CLICKHOUSE_DATABASE}"
dict_path="${CLICKHOUSE_DATABASE}.${dict_name}"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "
        DROP DICTIONARY IF EXISTS ${dict_path};
        DROP TABLE IF EXISTS ${table_name};
        DROP USER IF EXISTS ${user_name};
    "
}

trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "
    CREATE USER ${user_name};
    CREATE TABLE ${table_name} (id UInt64, value Float64) ENGINE = Memory;
    INSERT INTO ${table_name} VALUES (0, 0.5), (5, 1.5);
    CREATE DICTIONARY ${dict_path} (id UInt64, value Float64)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE '${table_name}' DB '${CLICKHOUSE_DATABASE}'))
    LAYOUT(FLAT()) LIFETIME(0);
"

$CLICKHOUSE_CLIENT --user "${user_name}" --query "SELECT dictGetOrNull('${dict_path}', 'value', NULL)" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "${user_name}" --query "SELECT dictGetOrNull('${dict_path}', 'value', materialize(CAST(NULL, 'Nullable(UInt64)')))" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "${user_name}" --query "SELECT dictGetOrNull('${dict_path}', 'value', CAST(NULL, 'Nullable(UInt64)'))" 2>&1 | grep -o -m1 'ACCESS_DENIED'

$CLICKHOUSE_CLIENT --query "GRANT dictGet ON ${dict_path} TO ${user_name}"

$CLICKHOUSE_CLIENT --user "${user_name}" --query "SELECT dictGetOrNull('${dict_path}', 'value', NULL), toTypeName(dictGetOrNull('${dict_path}', 'value', NULL))"
$CLICKHOUSE_CLIENT --user "${user_name}" --query "SELECT dictGetOrNull('${dict_path}', 'value', materialize(CAST(NULL, 'Nullable(UInt64)')))"
$CLICKHOUSE_CLIENT --user "${user_name}" --query "SELECT dictGetOrNull('${dict_path}', 'value', CAST(NULL, 'Nullable(UInt64)'))"
