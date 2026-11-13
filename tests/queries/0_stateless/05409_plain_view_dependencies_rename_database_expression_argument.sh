#!/usr/bin/env bash
# Regression test: `RENAME DATABASE` must keep the dependencies of a view whose source is named by a
# constant expression (here the table argument of `joinGet`). The dependencies are recomputed for the
# new database name before that database exists, so evaluating the expression must not require it.
#
# This is a shell test because the name of the source database has to be written inside the body of
# the view: a query parameter there would make it a parameterized view instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -ue

DB_BEFORE="${CLICKHOUSE_DATABASE}_before"
DB_AFTER="${CLICKHOUSE_DATABASE}_after"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB_BEFORE}"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB_AFTER}"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.j (id UInt64, val String) ENGINE = Join(ANY, LEFT, id)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${CLICKHOUSE_DATABASE}.j VALUES (1, 'kept')"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB_BEFORE} ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "CREATE VIEW ${DB_BEFORE}.v AS SELECT joinGet(concat('${CLICKHOUSE_DATABASE}.', 'j'), 'val', toUInt64(1)) AS val"

# The random database names are replaced by stable labels, so the reference file is deterministic.
dependents_of_j()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT arraySort(arrayMap((d, t) -> concat(multiIf(d = '${DB_BEFORE}', 'db_before', d = '${DB_AFTER}', 'db_after', d), '.', t), dependencies_database, dependencies_table))
        FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'j'"
}

# `check_referential_table_dependencies` is off by default, so ask for the check explicitly.
# The whole client output is captured, so a failing DROP does not leak to stderr.
drop_is_blocked()
{
    local output
    output=$($CLICKHOUSE_CLIENT --check_referential_table_dependencies=1 -q "DROP TABLE $1" 2>&1 || true)
    case "$output" in
        *HAVE_DEPENDENT_OBJECTS*) echo 'blocked' ;;
        '')                       echo 'DROPPED' ;;
        *)                        echo "unexpected: $output" ;;
    esac
}

echo -n 'before rename: '
dependents_of_j

$CLICKHOUSE_CLIENT -q "RENAME DATABASE ${DB_BEFORE} TO ${DB_AFTER}"

echo -n 'after rename: '
dependents_of_j

echo -n 'view reads: '
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${DB_AFTER}.v"

echo -n 'drop of the source: '
drop_is_blocked "${CLICKHOUSE_DATABASE}.j"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB_AFTER}"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${CLICKHOUSE_DATABASE}.j"
