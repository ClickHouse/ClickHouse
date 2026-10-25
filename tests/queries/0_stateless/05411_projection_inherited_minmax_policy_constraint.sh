#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: creates a user and a settings profile

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A projection inherits the implicit min-max index policy of its table. The inherited value is not
# a change made by the projection, so a `CONST` constraint on the policy must not reject a projection
# that does not set it, while an explicit override in `WITH SETTINGS` must still be rejected.

USER="u_05411_${CLICKHOUSE_DATABASE}"
PROFILE="p_05411_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_05411"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_05411_create"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_05411 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY b
    SETTINGS add_minmax_index_for_numeric_columns = 0"

$CLICKHOUSE_CLIENT -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS merge_tree_add_minmax_index_for_numeric_columns CONST TO ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT ALTER, SELECT ON ${CLICKHOUSE_DATABASE}.t_05411 TO ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.t_05411_create TO ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT TABLE ENGINE ON MergeTree TO ${USER}"

echo "no override"
$CLICKHOUSE_CLIENT --user "${USER}" -q "ALTER TABLE t_05411 ADD PROJECTION p (SELECT a ORDER BY a)"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.projections WHERE database = currentDatabase() AND table = 't_05411' ORDER BY name"

echo "override to the inherited value"
$CLICKHOUSE_CLIENT --user "${USER}" -q "
    ALTER TABLE t_05411 ADD PROJECTION p_same (SELECT a ORDER BY a) WITH SETTINGS (add_minmax_index_for_numeric_columns = 0)"

echo "override to another value"
$CLICKHOUSE_CLIENT --user "${USER}" -q "
    ALTER TABLE t_05411 ADD PROJECTION p_other (SELECT a ORDER BY a) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1)" 2>&1 \
    | grep -m1 -c -F "SETTING_CONSTRAINT_VIOLATION"

$CLICKHOUSE_CLIENT -q "SELECT name FROM system.projections WHERE database = currentDatabase() AND table = 't_05411' ORDER BY name"

# The same for a projection declared in `CREATE TABLE`, which is analyzed once without the table
# metadata before the storage is created: the check runs where the inherited policy is known.
echo "create: no override"
$CLICKHOUSE_CLIENT --user "${USER}" -q "
    CREATE TABLE t_05411_create (a UInt64, b UInt64, PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY b"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.projections WHERE database = currentDatabase() AND table = 't_05411_create' ORDER BY name"
$CLICKHOUSE_CLIENT --user "${USER}" -q "DROP TABLE t_05411_create"

echo "create: override to the inherited value"
$CLICKHOUSE_CLIENT --user "${USER}" -q "
    CREATE TABLE t_05411_create (a UInt64, b UInt64,
        PROJECTION p (SELECT a ORDER BY a) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1)) ENGINE = MergeTree ORDER BY b"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.projections WHERE database = currentDatabase() AND table = 't_05411_create' ORDER BY name"
$CLICKHOUSE_CLIENT --user "${USER}" -q "DROP TABLE t_05411_create"

echo "create: override to another value"
$CLICKHOUSE_CLIENT --user "${USER}" -q "
    CREATE TABLE t_05411_create (a UInt64, b UInt64,
        PROJECTION p (SELECT a ORDER BY a) WITH SETTINGS (add_minmax_index_for_numeric_columns = 0)) ENGINE = MergeTree ORDER BY b" 2>&1 \
    | grep -m1 -c -F "SETTING_CONSTRAINT_VIOLATION"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05411_create"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_05411"
$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${PROFILE}"
