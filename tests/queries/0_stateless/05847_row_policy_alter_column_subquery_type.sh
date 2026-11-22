#!/usr/bin/env bash
# a modify column must not change the type of what a subquery of a row policy reads

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE t (id UInt32, n UInt32, m UInt32, k ALIAS m) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
CREATE TABLE a ENGINE = Alias(t);
CREATE ROW POLICY pa ON $DB.a FOR SELECT USING id IN (SELECT id FROM $DB.t WHERE bitAnd(n, 1) = 1) TO CURRENT_USER;
CREATE ROW POLICY pt ON $DB.t FOR SELECT USING bitAnd(k, 1) = 1 TO CURRENT_USER;
SELECT 'before alias', arraySort(groupArray(id)) FROM a;
"

echo "-- a type change of a column read in a subquery"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t MODIFY COLUMN n String" 2>&1 | grep -o ALTER_OF_COLUMN_IS_FORBIDDEN | head -1
$CLICKHOUSE_CLIENT -q "SELECT 'after refused', arraySort(groupArray(id)) FROM a"

echo "-- a modify that keeps the type"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t MODIFY COLUMN n UInt32 DEFAULT 7"
$CLICKHOUSE_CLIENT -q "SELECT 'after default', arraySort(groupArray(id)) FROM a"

echo "-- an alias column keeps its type when its base column changes"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t MODIFY COLUMN m String SETTINGS mutations_sync = 2"
$CLICKHOUSE_CLIENT -q "SELECT 'after base', toTypeName(k), arraySort(groupArray(id)) FROM t GROUP BY toTypeName(k)"

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY pa ON $DB.a;
DROP ROW POLICY pt ON $DB.t;
DROP TABLE a;
DROP TABLE t;
"
