#!/usr/bin/env bash
# a policy that reads the altered table only in a subquery also guards its columns

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE

refused() { $CLICKHOUSE_CLIENT -q "$1" 2>&1 | grep -o "$2" | head -1; }

$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE t (id UInt32, tenant UInt32, kind UInt32, note String, code UInt32, code_alias UInt32 ALIAS code, extra UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 1, 1, 'a', 1, 0), (2, 2, 1, 'b', 2, 0), (3, 1, 2, 'c', 3, 0), (4, 1, 2, 'd', 5, 0);
CREATE TABLE a ENGINE = Alias(t);
CREATE TABLE a2 ENGINE = Alias(t);
CREATE ROW POLICY pa ON $DB.a FOR SELECT USING (SELECT max(tenant) FROM $DB.t) = 2 AND id IN (SELECT kind FROM $DB.a2) TO CURRENT_USER;
CREATE ROW POLICY pa2 ON $DB.a2 FOR SELECT USING id IN (SELECT number FROM numbers(10) WHERE number < length((SELECT groupArray(note) FROM $DB.t))) AND id IN (SELECT code_alias FROM $DB.t) TO CURRENT_USER;
SELECT 'before', arraySort(groupArray(id)) FROM a;
SELECT 'before a2', arraySort(groupArray(id)) FROM a2;
"

echo "-- a scalar subquery over the target"
refused "ALTER TABLE t DROP COLUMN tenant" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE t RENAME COLUMN tenant TO tenant2" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- an IN subquery over another alias of the target"
refused "ALTER TABLE a DROP COLUMN kind" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE a2 RENAME COLUMN kind TO kind2" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- a scalar subquery inside a subquery"
refused "ALTER TABLE t DROP COLUMN note" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE t RENAME COLUMN note TO comment" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- an alias column in a subquery, whose expression RENAME rewrites"
refused "ALTER TABLE t DROP COLUMN code_alias" ALTER_OF_COLUMN_IS_FORBIDDEN
$CLICKHOUSE_CLIENT -q "ALTER TABLE t RENAME COLUMN code TO code2"

echo "-- other columns are free"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t DROP COLUMN extra"
$CLICKHOUSE_CLIENT -q "SELECT 'after', arraySort(groupArray(id)) FROM a"
$CLICKHOUSE_CLIENT -q "SELECT 'after a2', arraySort(groupArray(id)) FROM a2"

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY pa ON $DB.a;
DROP ROW POLICY pa2 ON $DB.a2;
DROP TABLE a2;
DROP TABLE a;
DROP TABLE t;
"
