#!/usr/bin/env bash
# Regression test: a parameterized view called by a plain view, by a bare or a qualified name,
# lists the plain view in `system.tables.dependencies_*`.
#
# This is a shell test because a query parameter cannot be the database of a parameterized view call.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -ue

DB_OTHER="${CLICKHOUSE_DATABASE}_other"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB_OTHER}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB_OTHER} ENGINE = Atomic"

$CLICKHOUSE_CLIENT -n -q "
CREATE TABLE t (id UInt64) ENGINE = Memory;
INSERT INTO t VALUES (1), (2);

CREATE VIEW pv AS SELECT * FROM t WHERE id = {id:UInt64};
CREATE VIEW ${DB_OTHER}.pv_other AS SELECT * FROM ${CLICKHOUSE_DATABASE}.t WHERE id = {id:UInt64};

CREATE VIEW v AS SELECT * FROM pv(id = 1);
CREATE VIEW v_qualified AS SELECT * FROM ${DB_OTHER}.pv_other(id = 2);
"

$CLICKHOUSE_CLIENT -n -q "
SELECT name, dependencies_table FROM system.tables
WHERE database = currentDatabase() AND name = 'pv';
SELECT name, dependencies_database = [currentDatabase()], dependencies_table FROM system.tables
WHERE database = '${DB_OTHER}' AND name = 'pv_other';

SELECT * FROM v;
SELECT * FROM v_qualified;

DROP VIEW v;
DROP VIEW v_qualified;

SELECT 'after drop', name, dependencies_table FROM system.tables
WHERE (database = currentDatabase() AND name = 'pv') OR (database = '${DB_OTHER}' AND name = 'pv_other')
ORDER BY name;
"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB_OTHER}"
