#!/usr/bin/env bash
# the ALTER check for row policies counts the columns of every branch, also of branches that only some users or roles take

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE
ALICE="alice_${CLICKHOUSE_DATABASE}"
ROLE="r_${CLICKHOUSE_DATABASE}"

refused() { $CLICKHOUSE_CLIENT -q "$1" 2>&1 | grep -o "$2" | head -1; }

$CLICKHOUSE_CLIENT --multiquery -q "
DROP USER IF EXISTS $ALICE;
CREATE USER $ALICE;
CREATE TABLE t (id UInt32, tenant UInt32, team UInt32, other UInt32, note String, misc UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 1, 0, 0, '', 0), (2, 2, 0, 0, 'x', 0);
GRANT SELECT ON $DB.t TO $ALICE;
CREATE ROW POLICY pu ON $DB.t FOR SELECT USING if(currentUser() = '$ALICE', tenant = 1, note != '') TO ALL;
CREATE ROW POLICY pr ON $DB.t FOR SELECT USING has(currentRoles(), '$ROLE') AND team = 1 TO ALL;
CREATE ROW POLICY pe ON $DB.t FOR SELECT USING multiIf(has(enabledRoles(), '$ROLE'), other = 1, 0) TO ALL;
"

echo "-- the columns of the branches of other users and roles"
for column in tenant team other note; do
    refused "ALTER TABLE $DB.t DROP COLUMN $column" ALTER_OF_COLUMN_IS_FORBIDDEN
    refused "ALTER TABLE $DB.t RENAME COLUMN $column TO ${column}2" ALTER_OF_COLUMN_IS_FORBIDDEN
done
echo "-- a column that no policy reads"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $DB.t DROP COLUMN misc"
$CLICKHOUSE_CLIENT --user "$ALICE" -q "SELECT 'alice', groupArray(id) FROM $DB.t"
$CLICKHOUSE_CLIENT -q "SELECT 'default', groupArray(id) FROM $DB.t"

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY pu ON $DB.t;
DROP ROW POLICY pr ON $DB.t;
DROP ROW POLICY pe ON $DB.t;
DROP USER $ALICE;
DROP TABLE t;
"
