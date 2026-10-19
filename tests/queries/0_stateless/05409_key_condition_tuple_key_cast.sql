-- A `Tuple` comparison compares a `FixedString` element zero-padded with a `String` or `Dynamic` element, and any
-- element with a `Dynamic` one by value, but the cast of the key to the common type drops the padding or keeps the
-- type of the element, so partition pruning must not compare the casted key. The same holds for a `Tuple`, `Array`
-- or `Map` key compared with a `Dynamic` constant:
-- https://github.com/ClickHouse/ClickHouse/issues/126243

DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_int;
DROP TABLE IF EXISTS t_map;

CREATE TABLE t_tuple (k Tuple(FixedString(4), UInt8)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_tuple VALUES (('ab', 1));

SELECT 'tuple';
SELECT count() FROM t_tuple WHERE k = ('ab\0', 1);
SELECT count() FROM t_tuple WHERE k = (CAST('ab' AS Dynamic), 1);
SELECT countIf(k = ('ab\0', 1)), countIf(k = (CAST('ab' AS Dynamic), 1)) FROM t_tuple;
SELECT k FROM t_tuple WHERE k = ('ab\0', 1);
SELECT count() FROM t_tuple WHERE k IN (('ab\0', 1));
SELECT count() FROM t_tuple WHERE k = ('ac', 1);
SELECT count() FROM t_tuple WHERE k != ('ab\0', 1);
SELECT count() FROM t_tuple WHERE k = CAST(('ab\0', 1) AS Dynamic);
SELECT count() FROM t_tuple WHERE k <= CAST(('ab\0', 1) AS Dynamic);

CREATE TABLE t_nested (k Tuple(Tuple(FixedString(4), UInt8), String)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_nested VALUES ((('ab', 1), 'x'));

SELECT 'nested';
SELECT count() FROM t_nested WHERE k = (('ab\0', 1), 'x');
SELECT count() FROM t_nested WHERE k = ((CAST('ab' AS Dynamic), 1), 'x');
SELECT count() FROM t_nested WHERE k = (('ac', 1), 'x');

CREATE TABLE t_nullable (k Tuple(Nullable(FixedString(4)), UInt8)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple()
SETTINGS allow_nullable_key = 1;
INSERT INTO t_nullable VALUES (('ab', 1));

SELECT 'nullable';
SELECT count() FROM t_nullable WHERE k = ('ab\0', 1);
SELECT count() FROM t_nullable WHERE k = (CAST('ab' AS Dynamic), 1);
SELECT count() FROM t_nullable WHERE k = ('ac', 1);

CREATE TABLE t_int (k Tuple(UInt8, UInt8)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_int VALUES ((1, 1));

SELECT 'int';
SELECT count() FROM t_int WHERE k = (CAST(toInt64(1) AS Dynamic), 1);
SELECT count() FROM t_int WHERE k = (CAST(toInt64(2) AS Dynamic), 1);
SELECT countIf(k = (CAST(toInt64(1) AS Dynamic), 1)) FROM t_int;
SELECT count() FROM t_int WHERE k = CAST((toInt64(1), 1) AS Dynamic);
SELECT count() FROM t_int WHERE k = CAST((toInt64(2), 1) AS Dynamic);

-- An `Array` compares after a cast to the common type, so `['ab\0']` does not equal `[toFixedString('ab', 4)]`.
CREATE TABLE t_array (k Array(FixedString(4))) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_array VALUES (['ab']);

SELECT 'array';
SELECT count() FROM t_array WHERE k = ['ab\0'];
SELECT count() FROM t_array WHERE k = ['ab'];
SELECT countIf(k = ['ab\0']), countIf(k = ['ab']) FROM t_array;
SELECT count() FROM t_array WHERE k = CAST(['ab'] AS Dynamic);

CREATE TABLE t_map (k Map(String, UInt8)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_map VALUES ({'a': 1});

SELECT 'map';
SELECT count() FROM t_map WHERE k = CAST(map('a', toInt64(1)) AS Dynamic);
SELECT countIf(k = CAST(map('a', toInt64(1)) AS Dynamic)) FROM t_map;

DROP TABLE t_tuple;
DROP TABLE t_nested;
DROP TABLE t_nullable;
DROP TABLE t_array;
DROP TABLE t_int;
DROP TABLE t_map;
