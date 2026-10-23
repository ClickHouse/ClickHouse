-- `merge` infers `LowCardinality(String)` for enums numbered differently in different tables, so each table is read by name.

SELECT '-- Map keys';
CREATE TABLE metric_log (metrics Map(Enum16('ProfileEvent_Query' = 0, 'CurrentMetric_Query' = 5), Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE metric_log_0 (metrics Map(Enum16('CurrentMetric_Query' = 1, 'ProfileEvent_Query' = 2), Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE metric_log_1 (metrics Map(Enum16('CurrentMetric_Merge' = 0, 'ProfileEvent_Query' = 1), Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO metric_log VALUES ({'ProfileEvent_Query': 10, 'CurrentMetric_Query': 20});
INSERT INTO metric_log_0 VALUES ({'CurrentMetric_Query': 5, 'ProfileEvent_Query': 7});
INSERT INTO metric_log_1 VALUES ({'CurrentMetric_Merge': 3});
DESCRIBE merge(currentDatabase(), '^metric_log');
SELECT _table, metrics, metrics['CurrentMetric_Merge'] FROM merge(currentDatabase(), '^metric_log') ORDER BY _table;

SELECT '-- types that do not change';
CREATE TABLE same_1 (extended Enum8('a' = 1, 'b' = 2), disjoint Enum8('a' = 1), identical Enum8('a' = 1), with_string Enum8('a' = 1)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE same_2 (extended Enum8('a' = 1, 'b' = 2, 'c' = 3), disjoint Enum8('b' = 2), identical Enum8('a' = 1), with_string String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO same_1 (extended) VALUES ('b');
INSERT INTO same_2 (extended) VALUES ('b'), ('c');
DESCRIBE merge(currentDatabase(), '^same_');
SELECT _table, extended FROM merge(currentDatabase(), '^same_') ORDER BY ALL;
SELECT count() FROM merge(currentDatabase(), '^same_') WHERE extended = 2;

SELECT '-- conflicting numbering';
CREATE TABLE conflict_1 (renumbered Enum8('a' = 1, 'b' = 2), renamed Enum8('a' = 1), widened Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE conflict_2 (renumbered Enum8('a' = 2, 'b' = 1), renamed Enum8('b' = 1), widened Enum16('b' = 1, 'a' = 300)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO conflict_1 VALUES ('a', 'a', 'a'), ('b', 'a', 'b');
INSERT INTO conflict_2 VALUES ('a', 'b', 'a');
DESCRIBE merge(currentDatabase(), '^conflict_');
SELECT _table, renumbered, renamed, widened FROM merge(currentDatabase(), '^conflict_') ORDER BY ALL;
SELECT count() FROM merge(currentDatabase(), '^conflict_') WHERE renamed = 'b';

SELECT '-- containers, Nullable and a NULL source';
CREATE TABLE nested_1 (arr Array(Enum8('a' = 1, 'b' = 2)), tup Tuple(e Enum16('a' = 1, 'b' = 2), i UInt8), m Map(UInt8, Enum8('a' = 1, 'b' = 2)), n Nullable(Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE nested_2 (arr Array(Enum8('a' = 2, 'b' = 1)), tup Tuple(e Enum16('a' = 2, 'b' = 1), i UInt16), m Map(UInt8, Enum8('a' = 2, 'b' = 1)), n Enum8('a' = 2, 'b' = 1)) ENGINE = MergeTree ORDER BY tuple();
CREATE VIEW nested_3 AS SELECT NULL AS tup;
INSERT INTO nested_1 VALUES (['a', 'b'], ('a', 1), {1: 'a'}, 'a'), ([], ('b', 1), {}, NULL);
INSERT INTO nested_2 VALUES (['b'], ('b', 2), {2: 'b'}, 'b');
DESCRIBE merge(currentDatabase(), '^nested_');
SELECT _table, arr, tup, m, n FROM merge(currentDatabase(), '^nested_') ORDER BY _table, n;

SELECT '-- the conflict is only with the third table';
CREATE TABLE three_1 (x Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE three_2 (x Enum8('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE three_3 (x Enum8('b' = 1, 'a' = 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO three_1 VALUES ('a');
INSERT INTO three_2 VALUES ('c');
INSERT INTO three_3 VALUES ('a');
SELECT _table, x, toTypeName(x) FROM merge(currentDatabase(), '^three_') ORDER BY _table;

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

SELECT '-- a Merge table without columns, and the setting disabled';
CREATE TABLE merge_conflict ENGINE = Merge(currentDatabase(), '^conflict_');
DESCRIBE merge_conflict;
DESCRIBE merge(currentDatabase(), '^conflict_') SETTINGS merge_table_infer_conflicting_enums_as_string = 0;
