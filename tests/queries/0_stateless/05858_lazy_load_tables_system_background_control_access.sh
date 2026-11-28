#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: needs the `Kafka` engine.
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# `SYSTEM STOP|START|PAUSE|CANCEL|REFRESH db.table` requires `SYSTEM STREAMING ENGINES` for a streaming engine
# and `SYSTEM VIEWS` for a refreshable materialized view. For a deferred table of a `lazy_load_tables` database
# this is decided from the engine in the `CREATE` query, so a user who holds only the wrong grant is rejected
# before the table is loaded: loading constructs and starts it.

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
    CREATE TABLE \`$DB\`.streaming (x UInt64) ENGINE = Kafka
        SETTINGS kafka_broker_list = 'localhost:1', kafka_topic_list = 'topic_$DB', kafka_group_name = 'group_$DB', kafka_format = 'JSONEachRow';
    CREATE TABLE \`$DB\`.plain (x UInt64) ENGINE = MergeTree ORDER BY x;
    GRANT SYSTEM VIEWS ON \`$DB\`.* TO \`$USER\`;
    DETACH DATABASE \`$DB\` SYNC;
    ATTACH DATABASE \`$DB\`;
"

function is_loaded()
{
    $CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"
}

function run_as_user()
{
    echo "$1: $($CLICKHOUSE_CLIENT --user "$USER" -q "$1 \`$DB\`.$2" 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|BAD_ARGUMENTS' || echo OK)"
}

is_loaded

echo "SYSTEM VIEWS only, a streaming table"
for action in STOP START PAUSE CANCEL REFRESH
do
    run_as_user "SYSTEM $action" streaming
done
is_loaded

echo "SYSTEM VIEWS only, a table that is not a streaming one"
run_as_user "SYSTEM PAUSE" plain
is_loaded

echo "SYSTEM STREAMING ENGINES, a streaming table"
$CLICKHOUSE_CLIENT -q "GRANT SYSTEM STREAMING ENGINES ON \`$DB\`.streaming TO \`$USER\`"
run_as_user "SYSTEM PAUSE" streaming
run_as_user "SYSTEM START" streaming
is_loaded

$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "DROP USER \`$USER\`"
