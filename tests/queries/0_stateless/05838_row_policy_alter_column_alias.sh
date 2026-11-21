#!/usr/bin/env bash
# a read through an Alias applies the policies of the alias and of its target, so ALTER checks both ways

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE
DB2="${CLICKHOUSE_DATABASE}_2"

refused() { $CLICKHOUSE_CLIENT -q "$1" 2>&1 | grep -o "$2" | head -1; }

$CLICKHOUSE_CLIENT --multiquery -q "
DROP DATABASE IF EXISTS $DB2;
CREATE DATABASE $DB2;
CREATE TABLE t (id UInt32, tenant UInt32, note String, kind UInt8, extra UInt8, misc UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 1, 'a', 1, 0, 0), (2, 2, 'b', 1, 0, 0), (3, 1, 'c', 2, 0, 0), (4, 1, '', 1, 0, 0);
CREATE TABLE a ENGINE = Alias(t);
CREATE TABLE $DB2.a ENGINE = Alias($DB, t);
CREATE ROW POLICY pt ON $DB.t FOR SELECT USING tenant = 1 TO CURRENT_USER;
CREATE ROW POLICY pa ON $DB.a FOR SELECT USING length(a.note) > 0 TO CURRENT_USER;
CREATE ROW POLICY pdb2 ON $DB2.* FOR SELECT USING kind = 1 TO CURRENT_USER;
SELECT 'before', arraySort(groupArray(id)) FROM a;
SELECT 'before other database', arraySort(groupArray(id)) FROM $DB2.a;
"

echo "-- the policy of the target guards an ALTER through the alias"
refused "ALTER TABLE a DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE a RENAME COLUMN tenant TO tenant_id" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- the policy of an alias guards an ALTER of its target, and resolves against the name of the alias"
refused "ALTER TABLE t DROP COLUMN note" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE t RENAME COLUMN note TO comment" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- so does a database-wide policy of another alias of the target"
refused "ALTER TABLE t DROP COLUMN kind" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE a RENAME COLUMN kind TO kind2" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- other columns are free"
$CLICKHOUSE_CLIENT -q "ALTER TABLE a DROP COLUMN extra"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t RENAME COLUMN misc TO misc2"
$CLICKHOUSE_CLIENT -q "SELECT 'after', arraySort(groupArray(id)) FROM a"
$CLICKHOUSE_CLIENT -q "SELECT 'after other database', arraySort(groupArray(id)) FROM $DB2.a"

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY pt ON $DB.t;
DROP ROW POLICY pa ON $DB.a;
DROP ROW POLICY pdb2 ON $DB2.*;
DROP TABLE $DB2.a;
DROP TABLE a;
DROP TABLE t;
DROP DATABASE $DB2;
"
