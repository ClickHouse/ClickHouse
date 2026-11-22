#!/usr/bin/env bash
# a modify column must not remove or break what a row policy reads

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE

refused() { $CLICKHOUSE_CLIENT --allow_metadata_only_named_tuple_alter 1 -q "$1" 2>&1 | grep -o "$2" | head -1; }

$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE t (id UInt32, tp Tuple(a Int64, b String), tq Tuple(a Int64, b String), n UInt32, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, (1, 'x'), (1, 'x'), 1, 'a'), (2, (2, 'y'), (2, 'y'), 1, ''), (3, (3, 'x'), (3, 'z'), 1, 'c');
CREATE TABLE a ENGINE = Alias(t);
CREATE ROW POLICY p ON $DB.t FOR SELECT USING tp.b = 'x' AND tupleElement(tq, 'a') > 0 AND bitAnd(n, 1) = 1 AND length(s) > 0 TO CURRENT_USER;
CREATE ROW POLICY pa ON $DB.a FOR SELECT USING id IN (SELECT id FROM $DB.t WHERE tq.b = 'x') TO CURRENT_USER;
SELECT 'before', arraySort(groupArray(id)) FROM t;
SELECT 'before alias', arraySort(groupArray(id)) FROM a;
"

echo "-- a subcolumn read by name"
refused "ALTER TABLE t MODIFY COLUMN tp Tuple(a Int64)" ALTER_OF_COLUMN_IS_FORBIDDEN
refused "ALTER TABLE t MODIFY COLUMN tp Tuple(a Int64, c String)" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- a tuple element read by a function"
refused "ALTER TABLE t MODIFY COLUMN tq Tuple(b String)" "while checking row policy p ON $DB.t against the columns after ALTER TABLE"

echo "-- a type that the policy cannot read"
refused "ALTER TABLE t MODIFY COLUMN n String" "while checking row policy p ON $DB.t against the columns after ALTER TABLE"

echo "-- a subcolumn read in a subquery of an alias policy"
refused "ALTER TABLE t MODIFY COLUMN tq Tuple(a Int64, c String)" ALTER_OF_COLUMN_IS_FORBIDDEN

echo "-- a modify that keeps what the policies read"
$CLICKHOUSE_CLIENT --allow_metadata_only_named_tuple_alter 1 -q "ALTER TABLE t MODIFY COLUMN tp Tuple(a Int64, b String, c Nullable(Int64))"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t MODIFY COLUMN n UInt64"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t MODIFY COLUMN s String DEFAULT 'z'"
$CLICKHOUSE_CLIENT -q "SELECT 'after', arraySort(groupArray(id)) FROM t"
$CLICKHOUSE_CLIENT -q "SELECT 'after alias', arraySort(groupArray(id)) FROM a"

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY p ON $DB.t;
DROP ROW POLICY pa ON $DB.a;
DROP TABLE a;
DROP TABLE t;
"
