-- Index analysis of a Tuple, Array or Map key compared with a constant of other element types, issue #127663.

DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS t_nested_empty;
DROP TABLE IF EXISTS t_enum;
DROP TABLE IF EXISTS t_fixed_string;
DROP TABLE IF EXISTS t_fixed_string_nested;
DROP TABLE IF EXISTS t_fixed_string_coalesce;
DROP TABLE IF EXISTS t_fixed_string_array;
DROP TABLE IF EXISTS t_minmax;
DROP TABLE IF EXISTS t_partition;
DROP TABLE IF EXISTS t_partition_reverse;

CREATE TABLE t_tuple (a Tuple(DateTime('UTC'), Int32)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_tuple SELECT (toDateTime('2018-01-01 00:00:00', 'UTC') + number * 86400, 1) FROM numbers(10);

SELECT count() FROM t_tuple WHERE a = tuple(toDate('2018-01-05'), 1);
SELECT count() FROM t_tuple WHERE a != tuple(toDate('2018-01-05'), 1);
SELECT count() FROM t_tuple WHERE a < tuple(toDate('2018-01-05'), 0);
SELECT count() FROM t_tuple WHERE a >= tuple(toDate('2018-01-05'), 0);
SELECT count() FROM t_tuple WHERE tuple(toDateOrDefault('2018-01-05 00:00:00'), 0) < a;
SELECT count() FROM t_tuple WHERE NOT (tuple(toDateOrDefault('2018-01-05 00:00:00'), 0) < a);
SELECT count() FROM (EXPLAIN indexes = 1 SELECT * FROM t_tuple WHERE a = tuple(toDate('2018-01-05'), 1))
WHERE explain ILIKE '%Condition: (a in [(1515110400, 1), (1515110400, 1)])%';

CREATE TABLE t_array (a Array(DateTime('UTC'))) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_array SELECT [toDateTime('2018-01-01 00:00:00', 'UTC') + number * 86400] FROM numbers(10);

SELECT count() FROM t_array WHERE a = [toDate('2018-01-05')];
SELECT count() FROM t_array WHERE a >= [toDate('2018-01-05')];
SELECT count() FROM (EXPLAIN indexes = 1 SELECT * FROM t_array WHERE a = []) WHERE explain ILIKE '%Condition: (a in [[], []])%';

CREATE TABLE t_map (a Map(String, DateTime('UTC'))) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_map SELECT map('k', toDateTime('2018-01-01 00:00:00', 'UTC') + number * 86400) FROM numbers(10);

SELECT count() FROM t_map WHERE a = map('k', toDate('2018-01-05'));
SELECT count() FROM t_map WHERE a >= map('k', toDate('2018-01-05'));

CREATE TABLE t_nested_empty (a Tuple(Array(Array(String)), Map(String, Array(String)))) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_nested_empty SELECT tuple([[toString(number)]], map('k', [toString(number)])) FROM numbers(4);

SELECT count() FROM t_nested_empty WHERE a >= tuple([[]], map('k', []));
SELECT count() FROM (EXPLAIN indexes = 1 SELECT * FROM t_nested_empty WHERE a >= tuple([[]], map('k', []))) WHERE explain ILIKE '%Condition: (a in %';

CREATE TABLE t_enum (a Tuple(String, Int32)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_enum VALUES (('x', 1));

SELECT count() FROM t_enum WHERE a = tuple(CAST('x', 'Enum8(\'x\' = 1)'), 1);

-- A FixedString element of a tuple is compared zero-padded: 'abc', 'abc\0' and 'abc\0\0' all match.
CREATE TABLE t_fixed_string (a Tuple(String, Int32)) ENGINE = MergeTree PARTITION BY a ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_fixed_string VALUES (('abc', 1)), (('abc\0', 1)), (('abc\0\0', 1)), (('abd', 1));

SELECT count() FROM t_fixed_string WHERE a = tuple(toFixedString('abc', 5), 1);
SELECT count() FROM t_fixed_string WHERE a != tuple(toFixedString('abc', 5), 1);
SELECT count() FROM t_fixed_string WHERE a >= tuple(toFixedString('abc', 5), 1);
SELECT count() FROM t_fixed_string WHERE a = tuple(toNullable(toFixedString('abc', 5)), 1);
SELECT count() FROM t_fixed_string WHERE a = tuple(CAST(toFixedString('abc', 5), 'Variant(FixedString(5), UInt8)'), 1);
SELECT count() FROM t_fixed_string WHERE a = CAST(tuple(toFixedString('abc', 5), 1) AS Variant(Tuple(FixedString(5), UInt8), UInt8));
SELECT count() FROM t_fixed_string WHERE a = CAST(tuple(toFixedString('abc', 5), 1) AS Dynamic);

CREATE TABLE t_fixed_string_nested (a Tuple(Tuple(String), Int32)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_fixed_string_nested VALUES ((tuple('abc'), 1)), ((tuple('abc\0'), 1)), ((tuple('abc\0\0'), 1)), ((tuple('abd'), 1));

SELECT count() FROM t_fixed_string_nested WHERE a = tuple(tuple(toFixedString('abc', 5)), 1);

CREATE TABLE t_fixed_string_coalesce (a Tuple(String, Int32)) ENGINE = MergeTree ORDER BY coalesce(a) SETTINGS index_granularity = 1;
INSERT INTO t_fixed_string_coalesce VALUES (('abc', 1)), (('abc\0', 1)), (('abc\0\0', 1)), (('abd', 1));

SELECT count() FROM t_fixed_string_coalesce WHERE a <= tuple(toFixedString('abc', 5), 1);

-- An array is compared after a cast to Array(String), which drops the zero bytes: only ['abc'] matches.
CREATE TABLE t_fixed_string_array (a Array(String)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_fixed_string_array VALUES (['abc']), (['abc\0']), (['abc\0\0']), (['abd']);

SELECT count() FROM t_fixed_string_array WHERE a = [toFixedString('abc', 5)];
SELECT count() FROM (EXPLAIN indexes = 1 SELECT * FROM t_fixed_string_array WHERE a >= [toFixedString('abc', 5)])
WHERE explain ILIKE '%Condition: (a in [[''abc''], +Inf))%';

CREATE TABLE t_minmax (k UInt64, a Tuple(DateTime('UTC'), Int32), INDEX i a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_minmax SELECT number, (toDateTime('2018-01-01 00:00:00', 'UTC') + number * 86400, 1) FROM numbers(10);

SELECT k FROM t_minmax WHERE a = tuple(toDate('2018-01-05'), 1);

CREATE TABLE t_partition (k UInt64, a Tuple(DateTime('UTC'), Int32)) ENGINE = MergeTree PARTITION BY a ORDER BY k;
INSERT INTO t_partition SELECT number, (toDateTime('2018-01-01 00:00:00', 'UTC') + number * 86400, 1) FROM numbers(3);

SELECT k FROM t_partition WHERE a = tuple(toDate('2018-01-02'), 1);
SELECT k FROM t_partition WHERE a = CAST(tuple(toDate('2018-01-02'), 1) AS Dynamic);
SELECT k FROM t_partition WHERE a = tuple(CAST(toDate('2018-01-02') AS Dynamic), 1);

-- Partition pruning casts the key: a FixedString element is compared zero-padded, an Enum element by value.
CREATE TABLE t_partition_reverse (a Tuple(FixedString(5)), e Tuple(Enum8('z' = 1, 'a' = 2))) ENGINE = MergeTree PARTITION BY (a, e) ORDER BY tuple();
INSERT INTO t_partition_reverse VALUES (tuple('abc'), tuple('z')), (tuple('abd'), tuple('a'));

SELECT count() FROM t_partition_reverse WHERE a = tuple('abc\0');
SELECT count() FROM t_partition_reverse WHERE e >= tuple('z');

DROP TABLE t_tuple;
DROP TABLE t_array;
DROP TABLE t_map;
DROP TABLE t_nested_empty;
DROP TABLE t_enum;
DROP TABLE t_fixed_string;
DROP TABLE t_fixed_string_nested;
DROP TABLE t_fixed_string_coalesce;
DROP TABLE t_fixed_string_array;
DROP TABLE t_minmax;
DROP TABLE t_partition;
DROP TABLE t_partition_reverse;
