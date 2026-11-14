#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# A `DROP` that `ignore_drop_queries_probability` skips must still be authorized, and before the deferred table
# of a `lazy_load_tables` database is loaded to decide between skipping and truncating it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
USER="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS \`$DB\` SYNC;
    DROP USER IF EXISTS \`$USER\`;
    CREATE USER \`$USER\` IDENTIFIED WITH no_password;
    CREATE DATABASE \`$DB\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`$DB\`.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO \`$DB\`.t VALUES (7);"

$CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\`"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
$CLICKHOUSE_CLIENT -q "SELECT is_loaded FROM system.tables WHERE database = '$DB' AND name = 't'"

$CLICKHOUSE_CLIENT --user "$USER" -q "DROP TABLE \`$DB\`.t SETTINGS ignore_drop_queries_probability = 1" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "SELECT is_loaded FROM system.tables WHERE database = '$DB' AND name = 't'"

$CLICKHOUSE_CLIENT -q "GRANT DROP TABLE ON \`$DB\`.t TO \`$USER\`"
$CLICKHOUSE_CLIENT --user "$USER" -q "DROP TABLE \`$DB\`.t SETTINGS ignore_drop_queries_probability = 1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM \`$DB\`.t"

$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "DROP USER \`$USER\`"
