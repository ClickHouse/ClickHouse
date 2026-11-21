-- Regressions for `FixedArray`: the hash tables of keys, lightweight updates, lazily replicated columns,
-- the elements of a multidimensional `FixedArray` in `LEFT ARRAY JOIN` and in a correlated subquery,
-- the aliases in the lambda form of `einsum`, the cancellation of `einsum`, and the setting for tables.

SET enable_analyzer = 1;

SELECT '-- distinct';
SELECT DISTINCT [number, number + 1]::FixedArray(2, UInt8) AS x FROM numbers(3) ORDER BY x;
SELECT DISTINCT [[number % 2, 1], [2, 3]]::FixedArray(2, 2, UInt16) AS x FROM numbers(4) ORDER BY x;

SELECT '-- group by';
SELECT [number % 2, 1]::FixedArray(2, UInt32) AS k, count() FROM numbers(10) GROUP BY k ORDER BY k;

SELECT '-- in';
SELECT [number, number + 1]::FixedArray(2, UInt8) AS x FROM numbers(5)
WHERE x IN (SELECT [number * 2, number * 2 + 1]::FixedArray(2, UInt8) FROM numbers(3)) ORDER BY x;

SELECT '-- join';
SELECT a.x, b.y
FROM (SELECT [number, number + 1]::FixedArray(2, UInt8) AS x FROM numbers(4)) AS a
INNER JOIN (SELECT [number, number + 1]::FixedArray(2, UInt8) AS x, number AS y FROM numbers(1, 2)) AS b USING (x)
ORDER BY b.y;

SELECT '-- lightweight update';
SET allow_experimental_fixed_array_type = 1;
DROP TABLE IF EXISTS t_fixed_array_update;
CREATE TABLE t_fixed_array_update (id UInt64, v FixedArray(2, UInt32), m FixedArray(2, 2, UInt8))
ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_fixed_array_update SELECT number, [number, number], [[number, 0], [0, number]] FROM numbers(3);
UPDATE t_fixed_array_update SET v = [100, 200], m = [[1, 2], [3, 4]] WHERE id = 1;
SELECT * FROM t_fixed_array_update ORDER BY id;
DROP TABLE t_fixed_array_update;

SELECT '-- lazily replicated';
SELECT arrayConcat(a, [x])
FROM (SELECT [number, number + 1]::FixedArray(2, UInt64) AS a FROM numbers(2))
ARRAY JOIN [toUInt64(10), toUInt64(20)] AS x
ORDER BY ALL
SETTINGS enable_lazy_columns_replication = 1;

SELECT '-- left array join';
SELECT r, toTypeName(r) FROM (SELECT [[1, 2], [3, 4]]::FixedArray(2, 2, UInt8) AS m) LEFT ARRAY JOIN m AS r ORDER BY r;
SELECT emptyArrayToSingle([[1, 2], [3, 4]]::FixedArray(2, 2, UInt8)) AS e, toTypeName(e);

SELECT '-- correlated subquery';
SELECT mm, (SELECT toTypeName(mm)) FROM (SELECT arrayMap(r -> r, [[1, 2], [3, 4]]::FixedArray(2, 2, UInt8)) AS mm);

SELECT '-- einsum alias';
SELECT einsum(i -> (a[i] AS v) + v) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> (a[i] AS x1) * 10 + x1) FROM (SELECT [1, 2] AS a);

SELECT '-- einsum cancellation';
SELECT einsum('i,j->', (x, y) -> x + y, range(number + 1048576), range(number + 1048576))
FROM numbers(1) SETTINGS max_execution_time = 1; -- { serverError TIMEOUT_EXCEEDED }
SELECT einsum('i,j->', range(number + 1048576), range(number + 1048576))
FROM numbers(1) SETTINGS max_execution_time = 1; -- { serverError TIMEOUT_EXCEEDED }

SELECT '-- setting';
SET allow_experimental_fixed_array_type = 0;
DROP TABLE IF EXISTS t_fixed_array_setting;
CREATE TABLE t_fixed_array_setting (v FixedArray(2, UInt8)) ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_fixed_array_setting (v Array(FixedArray(2, UInt8))) ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_fixed_array_setting (v Array(UInt8)) ENGINE = Memory;
ALTER TABLE t_fixed_array_setting MODIFY COLUMN v FixedArray(2, UInt8); -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_fixed_array_setting ADD COLUMN w FixedArray(2, UInt8); -- { serverError ILLEGAL_COLUMN }
SELECT [1, 2]::FixedArray(2, UInt8);
DROP TABLE t_fixed_array_setting;
