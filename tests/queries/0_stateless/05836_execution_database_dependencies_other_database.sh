#!/usr/bin/env bash
# Regression test: the column expressions and the arguments of the table engine, including the function calls nested in them
# (`dictGet` in the sharding key of `Distributed`), are evaluated in the context of a query, so an unqualified dictionary name
# in them is resolved against the current database of the session rather than against the database owning the table.
# The referential dependency must point there too, also after a metadata-only `ALTER`, so that the dictionary cannot be dropped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -ue

OTHER_DB="${CLICKHOUSE_DATABASE}_other"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${OTHER_DB}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${OTHER_DB}"

$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY default_dict (id UInt64, val UInt64 DEFAULT 99)
    PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)"
$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY shard_dict (id UInt64, val UInt64 DEFAULT 0)
    PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${OTHER_DB}.t (id UInt64, val UInt64 DEFAULT dictGetUInt64(concat('default_', 'dict'), 'val', id))
    ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${OTHER_DB}.dist (id UInt64)
    ENGINE = Distributed('test_shard_localhost', '${OTHER_DB}', 'local', dictGetUInt64('shard_dict', 'val', id))"

function check_drop()
{
    for dict in default_dict shard_dict; do
        $CLICKHOUSE_CLIENT -q "DROP DICTIONARY ${dict} SETTINGS check_referential_table_dependencies = 1" 2>&1 \
            | grep -o -m1 "HAVE_DEPENDENT_OBJECTS" || echo "${dict} dropped"
    done
}

check_drop

echo "after ALTER"
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${OTHER_DB}.t MODIFY COMMENT 'c'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${OTHER_DB}.dist MODIFY COMMENT 'c'"
check_drop

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${OTHER_DB}"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY default_dict"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY shard_dict"
