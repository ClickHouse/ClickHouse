-- `merge` infers `LowCardinality(String)` for enums whose numbering conflicts between the tables,
-- so each table is read by name. Consistent numbering keeps the integer type.

SELECT '-- Map keys numbered differently';
CREATE TABLE metric_log (event_time DateTime, metrics Map(Enum16('ProfileEvent_Query' = 0, 'CurrentMetric_Query' = 5), Int64)) ENGINE = MergeTree ORDER BY event_time;
CREATE TABLE metric_log_0 (event_time DateTime, metrics Map(Enum16('CurrentMetric_Query' = 1, 'ProfileEvent_Query' = 2), Int64)) ENGINE = MergeTree ORDER BY event_time;
INSERT INTO metric_log VALUES ('2026-01-01 00:00:00', {'ProfileEvent_Query': 10, 'CurrentMetric_Query': 20});
CREATE TABLE metric_log_1 (event_time DateTime, metrics Map(Enum16('CurrentMetric_Merge' = 0, 'ProfileEvent_Query' = 1), Int64)) ENGINE = MergeTree ORDER BY event_time;
INSERT INTO metric_log_0 VALUES ('2026-01-01 00:00:00', {'CurrentMetric_Query': 5, 'ProfileEvent_Query': 7});
INSERT INTO metric_log_1 VALUES ('2026-01-01 00:00:00', {'CurrentMetric_Merge': 3});
SELECT DISTINCT toTypeName(metrics) FROM merge(currentDatabase(), '^metric_log(_[0-9]+)?$');
SELECT _table, metrics, metrics['ProfileEvent_Query'], metrics['CurrentMetric_Merge'] FROM merge(currentDatabase(), '^metric_log(_[0-9]+)?$') ORDER BY _table;

SELECT '-- one enum extends the other';
CREATE TABLE t_old (x Enum8('QueryStart' = 1, 'QueryFinish' = 2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_new (x Enum8('QueryStart' = 1, 'QueryFinish' = 2, 'ExceptionBeforeStart' = 3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_old VALUES ('QueryFinish');
INSERT INTO t_new VALUES ('QueryFinish'), ('ExceptionBeforeStart');
SELECT toTypeName(x), _table, x FROM merge(currentDatabase(), '^t_(old|new)$') ORDER BY _table, x;
SELECT count() FROM merge(currentDatabase(), '^t_(old|new)$') WHERE x = 2;

SELECT '-- disjoint names and numbers';
CREATE TABLE disjoint_1 (x Enum8('a' = 1)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE disjoint_2 (x Enum8('b' = 2)) ENGINE = MergeTree ORDER BY tuple();
DESCRIBE merge(currentDatabase(), '^disjoint_');

SELECT '-- identical enums';
CREATE TABLE same_1 (x Enum8('a' = 1, 'b' = 2), y UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE same_2 (x Enum8('a' = 1, 'b' = 2), y UInt16) ENGINE = MergeTree ORDER BY tuple();
DESCRIBE merge(currentDatabase(), '^same_');

SELECT '-- an enum and a String';
CREATE TABLE string_1 (x Enum8('a' = 1)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE string_2 (x String) ENGINE = MergeTree ORDER BY tuple();
DESCRIBE merge(currentDatabase(), '^string_');

SELECT '-- a name with different numbers';
CREATE TABLE e8_1 (x Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE e8_2 (x Enum8('a' = 2, 'b' = 1)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO e8_1 VALUES ('a'), ('b');
INSERT INTO e8_2 VALUES ('a');
DESCRIBE merge(currentDatabase(), '^e8_');
SELECT _table, x FROM merge(currentDatabase(), '^e8_') ORDER BY _table, x;
SELECT count() FROM merge(currentDatabase(), '^e8_') WHERE x = 'a';

SELECT '-- Enum8 and Enum16';
CREATE TABLE e16_1 (x Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE e16_2 (x Enum16('b' = 1, 'a' = 300)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO e16_1 VALUES ('a');
INSERT INTO e16_2 VALUES ('a'), ('b');
DESCRIBE merge(currentDatabase(), '^e16_');
SELECT _table, x FROM merge(currentDatabase(), '^e16_') ORDER BY _table, x;

SELECT '-- a number with different names';
CREATE TABLE names_1 (x Enum8('a' = 1)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE names_2 (x Enum8('b' = 1)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO names_1 VALUES ('a');
INSERT INTO names_2 VALUES ('b');
DESCRIBE merge(currentDatabase(), '^names_');
SELECT _table, x FROM merge(currentDatabase(), '^names_') ORDER BY _table;
SELECT count() FROM merge(currentDatabase(), '^names_') WHERE x = 'b';

SELECT '-- Array and Tuple';
CREATE TABLE nested_1 (arr Array(Enum8('a' = 1, 'b' = 2)), tup Tuple(e Enum16('a' = 1, 'b' = 2), n UInt8), m Map(UInt8, Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE nested_2 (arr Array(Enum8('a' = 2, 'b' = 1)), tup Tuple(e Enum16('a' = 2, 'b' = 1), n UInt16), m Map(UInt8, Enum8('a' = 2, 'b' = 1))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO nested_1 VALUES (['a', 'b'], ('a', 1), {1: 'a'});
INSERT INTO nested_2 VALUES (['b'], ('b', 2), {2: 'b'});
DESCRIBE merge(currentDatabase(), '^nested_');
SELECT _table, arr, tup, m FROM merge(currentDatabase(), '^nested_') ORDER BY _table;

SELECT '-- Nullable';
CREATE TABLE nullable_1 (x Nullable(Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE nullable_2 (x Enum8('a' = 2, 'b' = 1)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO nullable_1 VALUES ('a'), (NULL);
INSERT INTO nullable_2 VALUES ('b');
DESCRIBE merge(currentDatabase(), '^nullable_');
SELECT _table, x FROM merge(currentDatabase(), '^nullable_') ORDER BY _table, x;

SELECT '-- the conflict is only with the third table';
CREATE TABLE three_1 (x Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE three_2 (x Enum8('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE three_3 (x Enum8('b' = 1, 'a' = 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO three_1 VALUES ('a');
INSERT INTO three_2 VALUES ('c');
INSERT INTO three_3 VALUES ('a');
DESCRIBE merge(currentDatabase(), '^three_');
SELECT _table, x FROM merge(currentDatabase(), '^three_') ORDER BY _table;

SELECT '-- conflicting enums with a String or an integer';
-- `LowCardinality(String)` and `Int32` have no common type, so `i` keeps the type inferred without names.
CREATE TABLE other_1 (s Enum8('a' = 1), i Enum8('a' = 1)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE other_2 (s Enum8('b' = 1), i Enum8('b' = 1)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE other_3 (s String, i Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO other_1 VALUES ('a', 'a');
INSERT INTO other_2 VALUES ('b', 'b');
INSERT INTO other_3 VALUES ('c', 1000);
DESCRIBE merge(currentDatabase(), '^other_');
SELECT _table, s FROM merge(currentDatabase(), '^other_') WHERE NOT ignore(i) ORDER BY _table;

SELECT '-- a Merge table without columns';
CREATE TABLE merge_e8 ENGINE = Merge(currentDatabase(), '^e8_');
DESCRIBE merge_e8;
SELECT _table, x FROM merge_e8 ORDER BY _table, x;

SELECT '-- merge_table_infer_conflicting_enums_as_string = 0';
DESCRIBE merge(currentDatabase(), '^e8_') SETTINGS merge_table_infer_conflicting_enums_as_string = 0;
