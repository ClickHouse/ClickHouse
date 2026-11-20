#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# A grant on the exact name of a subcolumn is enough to read the subcolumn, and `count()`, from a table. The same
# must hold for a deferred table of a `lazy_load_tables` database that nobody has loaded yet.

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
    CREATE TABLE \`$DB\`.t (p Tuple(s String, o String)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO \`$DB\`.t VALUES (('a', 'b'));
    CREATE TABLE \`$DB\`.other (p Tuple(s String, o String)) ENGINE = MergeTree ORDER BY tuple();
    GRANT SELECT(\`p.s\`) ON \`$DB\`.t TO \`$USER\`;"

$CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
$CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"

$CLICKHOUSE_CLIENT --user "$USER" -q "SELECT p.s FROM \`$DB\`.t"
$CLICKHOUSE_CLIENT --user "$USER" -q "SELECT count() FROM \`$DB\`.t"

# Without any grant the table stays unloaded.
echo "other: $($CLICKHOUSE_CLIENT --user "$USER" -q "SELECT p.s FROM \`$DB\`.other" < /dev/null 2>&1 | grep -o -m1 'ACCESS_DENIED')"
$CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "DROP USER \`$USER\`"
