#!/usr/bin/env bash
# Regression test: after an `Alias('t')` is moved to another database, the authorization of the alias chain in
# `system.columns` must follow the effective target of every hop, so it reaches and checks the last table.
# The chain is `db2.a -> db2.src -> db1.src -> db_private.secret`, and the user can see the three aliases only.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -ue

DB1="${CLICKHOUSE_DATABASE}_1"
DB2="${CLICKHOUSE_DATABASE}_2"
DB_PRIVATE="${CLICKHOUSE_DATABASE}_private"
USER="${CLICKHOUSE_DATABASE}_limited"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
for db in "${DB1}" "${DB2}" "${DB_PRIVATE}"; do
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${db}"
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE ${db} ENGINE = Atomic"
done

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB_PRIVATE}.secret (secret_column UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB1}.a ENGINE = Alias('src')"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB2}.src ENGINE = Alias('${DB1}', 'src')"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB1}.src ENGINE = Alias('${DB_PRIVATE}', 'secret')"
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${DB1}.a TO ${DB2}.a"

$CLICKHOUSE_CLIENT -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS ON ${DB2}.a TO ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS ON ${DB2}.src TO ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS ON ${DB1}.src TO ${USER}"

columns_of_a()
{
    $CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT name FROM system.columns WHERE database = '${DB2}' AND table = 'a'"
}

echo 'without access to the last table:'
columns_of_a

$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS ON ${DB_PRIVATE}.secret TO ${USER}"

echo 'with access to the last table:'
columns_of_a

$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
for db in "${DB1}" "${DB2}" "${DB_PRIVATE}"; do
    $CLICKHOUSE_CLIENT -q "DROP DATABASE ${db}"
done
