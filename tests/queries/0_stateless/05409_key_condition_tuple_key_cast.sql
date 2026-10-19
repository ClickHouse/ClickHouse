-- Partition pruning must not compare a key cast to the common type when the cast changes the comparison:
-- https://github.com/ClickHouse/ClickHouse/issues/126243

DROP TABLE IF EXISTS t_fixed;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_enum;
DROP TABLE IF EXISTS t_int;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS t_named;
DROP TABLE IF EXISTS t_named_nullable;

-- A `FixedString` element compares zero-padded with a `String` or `Dynamic` element.
CREATE TABLE t_fixed (k Tuple(FixedString(4), Nullable(FixedString(4)))) ENGINE = MergeTree PARTITION BY k ORDER BY tuple()
SETTINGS allow_nullable_key = 1;
INSERT INTO t_fixed VALUES (('ab', 'cd'));
SELECT count() FROM t_fixed WHERE k = ('ab\0', 'cd');
SELECT count() FROM t_fixed WHERE k = ('ab', 'cd\0');
SELECT count() FROM t_fixed WHERE k = (CAST('ab' AS Dynamic), 'cd');
SELECT count() FROM t_fixed WHERE k = CAST(('ab\0', 'cd') AS Dynamic);

CREATE TABLE t_nested (k Tuple(Tuple(FixedString(4), UInt8), String)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_nested VALUES ((('ab', 1), 'x'));
SELECT count() FROM t_nested WHERE k = (('ab\0', 1), 'x');

-- An `Enum` element compares with a `String` element by the value of the element.
CREATE TABLE t_enum (k Tuple(Enum8('b' = 1, 'a' = 2), UInt8)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_enum VALUES (('a', 1)), (('b', 1));
SELECT count() FROM t_enum WHERE k > ('b', 1);

-- Values of different types in a `Dynamic` compare by the type.
CREATE TABLE t_int (k Tuple(UInt8, UInt8)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_int VALUES ((1, 1));
SELECT count() FROM t_int WHERE k = (CAST(toInt64(1) AS Dynamic), 1);
SELECT count() FROM t_int WHERE k = CAST((toInt64(1), 1) AS Dynamic);

CREATE TABLE t_array (k Array(UInt8)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_array VALUES ([0]), ([2]);
SELECT count() FROM t_array WHERE k = CAST([toInt64(2)] AS Dynamic);
SELECT count() FROM t_array WHERE k > [CAST(toInt64(1) AS Dynamic)];

CREATE TABLE t_map (k Map(String, UInt8)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_map VALUES ({'a': 1});
SELECT count() FROM t_map WHERE k = CAST(map('a', toInt64(1)) AS Dynamic);

-- Converting the constant to the key type instead must pair named tuple elements by position, as the comparison.
CREATE TABLE t_named (k Tuple(a FixedString(4), b FixedString(4))) ENGINE = MergeTree PARTITION BY identity(k) ORDER BY tuple();
INSERT INTO t_named VALUES (('ab', 'cd'));
SELECT count() FROM t_named WHERE identity(k) = CAST(('ab', 'cd'), 'Tuple(b String, a String)');
SELECT count() FROM t_named WHERE k = CAST(('ab', 'cd'), 'Tuple(b String, a String)');

CREATE TABLE t_named_nullable (k Nullable(Tuple(a String, b String))) ENGINE = MergeTree PARTITION BY assumeNotNull(k) ORDER BY tuple();
INSERT INTO t_named_nullable VALUES (('ab', 'cd'));
SELECT count() FROM t_named_nullable WHERE k = CAST(('ab', 'cd'), 'Tuple(b String, a String)');

DROP TABLE t_fixed;
DROP TABLE t_nested;
DROP TABLE t_enum;
DROP TABLE t_int;
DROP TABLE t_array;
DROP TABLE t_map;
DROP TABLE t_named;
DROP TABLE t_named_nullable;
