#!/usr/bin/env bash
# The RESTORE creation modes must survive the ON CLUSTER round trip, and their Bool literals must parse.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

$CLICKHOUSE_CLIENT -m --query "
CREATE TABLE ${CLICKHOUSE_DATABASE}.t (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}.t SELECT 1;
CREATE TABLE ${CLICKHOUSE_DATABASE}.t2 AS ${CLICKHOUSE_DATABASE}.t;
CREATE TABLE ${CLICKHOUSE_DATABASE}.t3 AS ${CLICKHOUSE_DATABASE}.t;
"
$CLICKHOUSE_CLIENT --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup_name" | grep -o "BACKUP_CREATED"

function restore()
{
    local target=$1 on_cluster=$2 settings=$3
    $CLICKHOUSE_CLIENT --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.$target $on_cluster FROM $backup_name SETTINGS $settings" 2>&1 \
        | grep -m1 -oE 'RESTORED|UNKNOWN_TABLE|TABLE_ALREADY_EXISTS|CANNOT_PARSE_BACKUP_SETTINGS'
}

restore t1 "" "create_table = true, create_access = true, create_function = true"
restore t2 "" "create_table = false, create_database = false"
restore t1 "" "create_table = true"
restore t6 "" "create_table = false"
restore t3 "ON CLUSTER test_shard_localhost" "create_table = 'must-exist', create_database = 'must-exist'"
restore t4 "ON CLUSTER test_shard_localhost" "create_table = 'must-exist'"
restore t5 "ON CLUSTER test_shard_localhost" "create_table = 'create', create_access = 'create', create_function = 'create', create_named_collection = 'create', create_workloads_and_resources = 'create'"
restore t5 "ON CLUSTER test_shard_localhost" "create_table = 'create'"

for target in t1 t2 t3 t5; do
    $CLICKHOUSE_CLIENT --query "SELECT '$target', count() FROM ${CLICKHOUSE_DATABASE}.$target"
done
