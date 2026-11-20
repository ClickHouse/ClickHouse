#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# An `Alias` to a deferred table of a `lazy_load_tables` database loads the target when it is asked for its metadata
# or engine facts, before it checks the rights on the target. So a user with rights on the alias only must not be
# able to load the target through it, while `SHOW COLUMNS` and `SHOW INDEXES` on the alias load it to report the keys.

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
    CREATE TABLE \`$DB\`.t (x UInt64, y UInt64, INDEX idx y TYPE minmax) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE alias ENGINE = Alias(\`$DB\`, t) SETTINGS allow_experimental_alias_table_engine = 1;
    GRANT SELECT, INSERT, ALTER, SHOW ON ${CLICKHOUSE_DATABASE}.alias TO \`$USER\`;
"

function reattach()
{
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\` SYNC"
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
}

function is_loaded()
{
    $CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' AND name = 't'"
}

reattach
for query in \
    "SELECT count() FROM alias" \
    "EXPLAIN QUERY TREE SELECT * FROM alias" \
    "INSERT INTO alias VALUES (1, 1)" \
    "INSERT INTO alias SETTINGS async_insert = 1 VALUES (1, 1)" \
    "ALTER TABLE alias DELETE WHERE x = 1" \
    "ALTER TABLE alias UPDATE y = 2 WHERE x = 1" \
    "DELETE FROM alias WHERE x = 1"
do
    echo "$($CLICKHOUSE_CLIENT --user "$USER" -q "$query" < /dev/null 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|UNKNOWN_TABLE')"
done
is_loaded

# `SHOW COLUMNS` and `SHOW INDEXES` without the rights on the target do not load it.
$CLICKHOUSE_CLIENT --user "$USER" -q "SHOW COLUMNS FROM alias" > /dev/null 2>&1
$CLICKHOUSE_CLIENT --user "$USER" -q "SHOW INDEXES FROM alias" > /dev/null 2>&1
is_loaded

# With the rights on the target, they load it and report its keys and indices.
$CLICKHOUSE_CLIENT -q "SHOW COLUMNS FROM alias"
is_loaded
reattach
$CLICKHOUSE_CLIENT -q "SHOW INDEXES FROM alias"
is_loaded

# With the right on the target, the user loads it through the alias.
reattach
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON \`$DB\`.t TO \`$USER\`"
$CLICKHOUSE_CLIENT --user "$USER" -q "SELECT count() FROM alias"
is_loaded

$CLICKHOUSE_CLIENT -q "DROP TABLE alias SYNC"
$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "DROP USER \`$USER\`"
