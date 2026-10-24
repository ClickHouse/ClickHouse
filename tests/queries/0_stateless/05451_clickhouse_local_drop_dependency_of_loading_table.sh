#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local loads the persisted tables asynchronously, and the dependencies of a table on
# named collections and definers are registered only when the table is loaded. `DROP NAMED COLLECTION`
# and `DROP USER` as the first statement of a new process must wait for the tables that are still
# loading instead of dropping an object that a persisted table depends on.
# A `Set` table with many entries takes a noticeable time to load, and the dependent tables
# wait for it because of the `IN` in their column defaults.

data_dir="${CLICKHOUSE_TMP}/data_${CLICKHOUSE_DATABASE}"
rm -rf "$data_dir"

$CLICKHOUSE_LOCAL --path "$data_dir" --query "
    CREATE TABLE slow (x UInt64) ENGINE = Set;
    INSERT INTO slow SELECT number FROM numbers(1000000);
    CREATE NAMED COLLECTION nc_${CLICKHOUSE_DATABASE} AS url = 'http://127.0.0.1:1/data.csv', format = 'CSV';
    CREATE TABLE dep (x UInt64, present UInt8 DEFAULT x IN slow) ENGINE = URL(nc_${CLICKHOUSE_DATABASE});
    CREATE USER owner_${CLICKHOUSE_DATABASE};
    CREATE VIEW v (x UInt64 DEFAULT 1 IN slow) DEFINER = owner_${CLICKHOUSE_DATABASE} SQL SECURITY DEFINER AS SELECT toUInt64(1) AS x;
"

$CLICKHOUSE_LOCAL --path "$data_dir" --query "DROP NAMED COLLECTION nc_${CLICKHOUSE_DATABASE}" 2>&1 | grep -o -m1 'NAMED_COLLECTION_IS_USED'
$CLICKHOUSE_LOCAL --path "$data_dir" --query "DROP USER owner_${CLICKHOUSE_DATABASE}" 2>&1 | grep -o -m1 'HAVE_DEPENDENT_OBJECTS'

$CLICKHOUSE_LOCAL --path "$data_dir" --query "
    SELECT count() FROM system.named_collections WHERE name = 'nc_${CLICKHOUSE_DATABASE}';
    SELECT * FROM v;
"

rm -rf "$data_dir"
