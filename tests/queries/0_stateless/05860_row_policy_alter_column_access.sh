#!/usr/bin/env bash
# the ALTER check for row policies analyzes a policy with the grants of the user who alters the table, except for the tables the policy reads

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE
DDL_USER="u_${CLICKHOUSE_DATABASE}"
FILE="${CLICKHOUSE_DATABASE}_ids.tsv"
echo 1 > "$USER_FILES_PATH/$FILE"

refused() { $CLICKHOUSE_CLIENT "${@:3}" -q "$1" 2>&1 | grep -o "$2" | head -1; }

$CLICKHOUSE_CLIENT --multiquery -q "
DROP USER IF EXISTS $DDL_USER;
CREATE USER $DDL_USER;
CREATE TABLE allowed (id UInt32) ENGINE = Memory;
CREATE TABLE ttf (id UInt32, tenant UInt32, extra UInt8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tfn (id UInt32, extra UInt8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tin (id UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE names (name String) ENGINE = Memory;
INSERT INTO names VALUES ('$FILE');
CREATE TABLE tsq (id UInt32, tenant UInt32, extra UInt8) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY ptf ON $DB.ttf FOR SELECT USING tenant IN (SELECT id FROM file('$FILE', 'TSV', 'id UInt32')) TO CURRENT_USER;
CREATE ROW POLICY pfn ON $DB.tfn FOR SELECT USING id > length(file('$FILE')) TO CURRENT_USER;
CREATE ROW POLICY pin ON $DB.tin FOR SELECT USING tenant IN (SELECT id FROM $DB.allowed) TO CURRENT_USER;
CREATE ROW POLICY psq ON $DB.tsq FOR SELECT USING tenant IN (SELECT id FROM file((SELECT any(name) FROM $DB.names), 'TSV', 'id UInt32')) TO CURRENT_USER;
GRANT ALTER DROP COLUMN, ALTER MODIFY COLUMN ON $DB.* TO $DDL_USER;
"

echo "-- a table function or the function file in a policy needs the grants of the user who alters the table"
refused "ALTER TABLE $DB.ttf DROP COLUMN extra" ACCESS_DENIED --user "$DDL_USER"
refused "ALTER TABLE $DB.tfn DROP COLUMN extra" ACCESS_DENIED --user "$DDL_USER"
$CLICKHOUSE_CLIENT --multiquery -q "GRANT CREATE TEMPORARY TABLE ON *.* TO $DDL_USER; GRANT READ ON FILE TO $DDL_USER"
$CLICKHOUSE_CLIENT --user "$DDL_USER" -q "ALTER TABLE $DB.ttf DROP COLUMN extra"
$CLICKHOUSE_CLIENT --user "$DDL_USER" -q "ALTER TABLE $DB.tfn DROP COLUMN extra"
refused "ALTER TABLE $DB.ttf DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN --user "$DDL_USER"

echo "-- the check runs a subquery in the arguments of a table function, which needs the grants of the user"
refused "ALTER TABLE $DB.tsq DROP COLUMN extra" ACCESS_DENIED --user "$DDL_USER"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $DB.names TO $DDL_USER"
$CLICKHOUSE_CLIENT --user "$DDL_USER" -q "ALTER TABLE $DB.tsq DROP COLUMN extra"

echo "-- the tables a policy reads need no grant"
refused "SELECT * FROM $DB.allowed" ACCESS_DENIED --user "$DDL_USER"
$CLICKHOUSE_CLIENT --user "$DDL_USER" -q "ALTER TABLE $DB.tin MODIFY COLUMN tenant UInt64"
$CLICKHOUSE_CLIENT -q "SELECT table, name, type FROM system.columns WHERE database = '$DB' AND table IN ('ttf', 'tfn', 'tin', 'tsq') ORDER BY table, position"

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY ptf ON $DB.ttf;
DROP ROW POLICY pfn ON $DB.tfn;
DROP ROW POLICY pin ON $DB.tin;
DROP ROW POLICY psq ON $DB.tsq;
DROP USER $DDL_USER;
"
rm -f "${USER_FILES_PATH:?}/$FILE"
