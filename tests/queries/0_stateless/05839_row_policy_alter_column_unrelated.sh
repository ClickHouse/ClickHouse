#!/usr/bin/env bash
# the ALTER check for row policies does not refuse changes of columns that no policy reads

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE
DDL_USER="u_${CLICKHOUSE_DATABASE}"

refused() { $CLICKHOUSE_CLIENT "${@:3}" -q "$1" 2>&1 | grep -o "$2" | head -1; }

$CLICKHOUSE_CLIENT --multiquery -q "
DROP USER IF EXISTS $DDL_USER;
CREATE USER $DDL_USER;
CREATE TABLE allowed (id UInt32) ENGINE = Memory;
CREATE TABLE t (id UInt32, tenant UInt32, extra UInt8, misc UInt8, other UInt8) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pin ON $DB.t FOR SELECT USING tenant IN (SELECT id FROM $DB.allowed) TO CURRENT_USER;
CREATE ROW POLICY pscalar ON $DB.t FOR SELECT USING id = (SELECT max(id) FROM $DB.allowed) TO CURRENT_USER;
GRANT ALTER DROP COLUMN, ALTER RENAME COLUMN ON $DB.t TO $DDL_USER;
"

echo "-- a user who cannot read the lookup table of the policy can alter other columns"
refused "SELECT * FROM $DB.allowed" ACCESS_DENIED --user "$DDL_USER"
$CLICKHOUSE_CLIENT --user "$DDL_USER" -q "ALTER TABLE $DB.t DROP COLUMN extra"
$CLICKHOUSE_CLIENT --user "$DDL_USER" -q "ALTER TABLE $DB.t RENAME COLUMN misc TO misc2"
echo "-- but not the columns the policy reads"
refused "ALTER TABLE $DB.t DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN --user "$DDL_USER"
refused "ALTER TABLE $DB.t RENAME COLUMN id TO id2" ALTER_OF_COLUMN_IS_FORBIDDEN --user "$DDL_USER"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.columns WHERE database = '$DB' AND table = 't' ORDER BY position"

echo "-- a column with a dot in its name is not a subcolumn of another column"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE td (id UInt32, a UInt32, \`a.b\` UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO td VALUES (1, 1, 1), (2, 1, 2);
CREATE ROW POLICY pd ON $DB.td FOR SELECT USING \`a.b\` = 1 TO CURRENT_USER;
ALTER TABLE td RENAME COLUMN a TO a2;
ALTER TABLE td DROP COLUMN a2;
SELECT 'dotted column policy', arraySort(groupArray(id)) FROM td;
"
refused "ALTER TABLE td DROP COLUMN \`a.b\`" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE td RENAME COLUMN \`a.b\` TO \`a.c\`" ALTER_OF_COLUMN_IS_FORBIDDEN

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY pin ON $DB.t;
DROP ROW POLICY pscalar ON $DB.t;
DROP ROW POLICY pd ON $DB.td;
DROP USER $DDL_USER;
DROP TABLE t;
DROP TABLE allowed;
DROP TABLE td;
"
