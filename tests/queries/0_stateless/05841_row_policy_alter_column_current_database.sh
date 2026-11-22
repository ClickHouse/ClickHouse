#!/usr/bin/env bash
# a policy subquery column that binds to the policy table in the current database of a reader is correlated: that read fails also before the ALTER

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE
DB2=$CLICKHOUSE_DATABASE_2

refused() { $CLICKHOUSE_CLIENT -q "USE $1; $2" 2>&1 | grep -o "$3" | head -1; }

$CLICKHOUSE_CLIENT --multiquery -q "
DROP DATABASE IF EXISTS $DB2;
CREATE DATABASE $DB2;
CREATE TABLE $DB.t (id UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO $DB.t VALUES (1, 1), (2, 2);
CREATE TABLE $DB.allowed (tenant UInt32) ENGINE = MergeTree ORDER BY tenant;
INSERT INTO $DB.allowed VALUES (1);
CREATE TABLE $DB2.allowed (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO $DB2.allowed VALUES (1);
CREATE ROW POLICY p ON $DB.t FOR SELECT USING EXISTS (SELECT 1 FROM allowed WHERE tenant = 1) TO ALL;
"

echo "-- before the ALTER: tenant of allowed of the test database, then tenant of t from the second database"
$CLICKHOUSE_CLIENT -q "SELECT groupArray(id) FROM $DB.t"
refused "$DB2" "SELECT groupArray(id) FROM $DB.t" NOT_IMPLEMENTED
echo "-- the ALTER from the second database counts tenant of t"
refused "$DB2" "ALTER TABLE $DB.t DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
echo "-- the ALTER from the test database"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $DB.t DROP COLUMN tenant"
$CLICKHOUSE_CLIENT -q "SELECT groupArray(id) FROM $DB.t"

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY p ON $DB.t;
DROP DATABASE $DB2;
"
