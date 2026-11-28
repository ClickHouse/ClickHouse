#!/usr/bin/env bash
# Regression test: the name of a `joinGet` table, an expression-valued `dictGet` name and a bare parameterized view call
# in the query of a view are not qualified in the stored definition and are resolved against the current database
# of the query which reads the view, not against the database owning the view. The dependencies must point there too,
# also after the view is moved to another database, so that the actual sources cannot be dropped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -ue

VIEW_DB="${CLICKHOUSE_DATABASE}_view"
MOVED_DB="${CLICKHOUSE_DATABASE}_moved"
RENAMED_DB="${CLICKHOUSE_DATABASE}_renamed"

for db in "${VIEW_DB}" "${MOVED_DB}" "${RENAMED_DB}"; do
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${db}"
done
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${VIEW_DB} ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${MOVED_DB} ENGINE = Atomic"

# The same names in the current database (which reads the view) and in the database of the view.
for db in "${CLICKHOUSE_DATABASE}" "${VIEW_DB}"; do
    src=$([ "${db}" = "${CLICKHOUSE_DATABASE}" ] && echo reader || echo view)
    $CLICKHOUSE_CLIENT -m -q "
        CREATE TABLE ${db}.j (id UInt64, val String) ENGINE = Join(ANY, LEFT, id);
        INSERT INTO ${db}.j VALUES (1, '${src}');
        CREATE DICTIONARY ${db}.d (id UInt64, val String DEFAULT '${src}')
            PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);
        CREATE VIEW ${db}.pv AS SELECT {id:UInt64} AS id, '${src}' AS val;
    "
done

$CLICKHOUSE_CLIENT -m -q "
    CREATE VIEW ${VIEW_DB}.v AS
        SELECT joinGet(concat('', 'j'), 'val', toUInt64(1)) AS j, dictGetString(concat('', 'd'), 'val', toUInt64(1)) AS d;
    CREATE VIEW ${VIEW_DB}.v_pv AS SELECT val FROM pv(id = 1);
    SELECT * FROM ${VIEW_DB}.v;
    SELECT * FROM ${VIEW_DB}.v_pv;
"

function check()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT if(database = currentDatabase(), 'reader', 'view'), name, arraySort(dependencies_table)
        FROM system.tables
        WHERE database IN (currentDatabase(), '${VIEW_DB}') AND name IN ('j', 'd', 'pv')
        ORDER BY 1, 2"

    for object in "TABLE j" "DICTIONARY d" "VIEW pv"; do
        $CLICKHOUSE_CLIENT -q "DROP ${object} SETTINGS check_referential_table_dependencies = 1" 2>&1 \
            | grep -o -m1 "HAVE_DEPENDENT_OBJECTS" || echo "${object} dropped"
    done
}

echo "create"
check

echo "rename table"
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${VIEW_DB}.v TO ${MOVED_DB}.v, ${VIEW_DB}.v_pv TO ${MOVED_DB}.v_pv"
check

echo "rename database"
$CLICKHOUSE_CLIENT -q "RENAME DATABASE ${MOVED_DB} TO ${RENAMED_DB}"
check

$CLICKHOUSE_CLIENT -q "SELECT * FROM ${RENAMED_DB}.v"
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${RENAMED_DB}.v_pv"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${RENAMED_DB}"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${VIEW_DB}"
