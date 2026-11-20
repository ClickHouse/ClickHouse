#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# Before a distributed query, the initiator sends a `TablesStatusRequest` for the remote table over the connection of
# the remote user. A replicated table that a database with `lazy_load_tables` has not loaded yet is loaded to report
# its delay, so a user without rights on it must get `ACCESS_DENIED` and the table must stay unloaded.

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
    CREATE TABLE \`$DB\`.r (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', '1') ORDER BY x;
    -- The column list is given, so the remote structure is not requested with \`DESCRIBE\`.
    CREATE TABLE ${CLICKHOUSE_DATABASE}.rt (x UInt64) AS remote('127.0.0.2', '$DB', 'r', '$USER');"

$CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"

# `prefer_localhost_replica = 0` makes the query connect to the local server, so the status request is sent.
# The initiator reports the `ACCESS_DENIED` of the remote server in different ways (e.g. with hedged requests, as a failed
# connection), so only the failure is checked. Without the check, the table would be loaded before the query fails.
$CLICKHOUSE_CLIENT --prefer_localhost_replica 0 -q "SELECT count() FROM ${CLICKHOUSE_DATABASE}.rt" > /dev/null 2>&1 && echo "no rights: succeeded" || echo "no rights: failed"
$CLICKHOUSE_CLIENT -q "SELECT 'no rights', is_loaded FROM system.tables WHERE database = '$DB' AND name = 'r'"

# With the right, the table is loaded.
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON \`$DB\`.r TO \`$USER\`"
$CLICKHOUSE_CLIENT --prefer_localhost_replica 0 -q "SELECT 'granted', count() FROM ${CLICKHOUSE_DATABASE}.rt"
$CLICKHOUSE_CLIENT -q "SELECT 'granted', is_loaded FROM system.tables WHERE database = '$DB' AND name = 'r'"

$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "DROP USER \`$USER\`"
