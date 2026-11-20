#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# A user without rights on a deferred table of a `lazy_load_tables` database must not be able to load it
# through `SELECT`, `INSERT`, `ALTER` or `SYSTEM CANCEL`: loading constructs and starts the storage. The error is
# `ACCESS_DENIED` both for a valid table and for one whose construction fails, which must not reveal its definition.

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
    CREATE TABLE \`$DB\`.ref (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE \`$DB\`.valid (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE \`$DB\`.broken (x UInt64, CONSTRAINT c CHECK x < (SELECT max(x) FROM \`$DB\`.ref)) ENGINE = MergeTree ORDER BY x;"

$CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE \`$DB\`.ref SYNC"

for table in valid broken
do
    for query in \
        "SELECT * FROM \`$DB\`.$table" \
        "SELECT 1 WHERE 1 IN (SELECT x FROM \`$DB\`.$table)" \
        "INSERT INTO \`$DB\`.$table VALUES (1)" \
        "ALTER TABLE \`$DB\`.$table ADD COLUMN v UInt64" \
        "SYSTEM CANCEL \`$DB\`.$table"
    do
        echo "$table: $($CLICKHOUSE_CLIENT --user "$USER" -q "$query" < /dev/null 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|UNKNOWN_TABLE')"
    done
done

$CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"

# With the right, the table is loaded.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(x) ON \`$DB\`.valid TO \`$USER\`"
$CLICKHOUSE_CLIENT --user "$USER" -q "SELECT count() FROM \`$DB\`.valid"
$CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"

# `DROP DATABASE` loads the tables, so the one `broken` depends on is restored first.
$CLICKHOUSE_CLIENT -q "CREATE TABLE \`$DB\`.ref (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "DROP USER \`$USER\`"
