-- The data type FixedArray: an array of a fixed size, possibly multidimensional.

SELECT '-- names';
SELECT toTypeName([1, 2, 3]::FixedArray(3, UInt8));
SELECT toTypeName([[1, 2], [3, 4], [5, 6]]::FixedArray(3, 2, Float32));
SELECT toTypeName([[1, 2], [3, 4], [5, 6]]::FixedArray(3, FixedArray(2, Float32)));
SELECT toTypeName(CAST([['a'], ['b']], 'FixedArray(2, 1, LowCardinality(String))'));
SELECT toTypeName([(1, 'a')]::FixedArray(1, Tuple(x UInt8, s String)));
SELECT toTypeName([NULL, 1]::FixedArray(2, Nullable(UInt8)));

SELECT '-- values';
SELECT [1, 2, 3]::FixedArray(3, UInt8) AS x, x[1], x[3], x[-1], x[4];
SELECT [[1, 2, 3], [4, 5, 6]]::FixedArray(2, 3, Int16) AS m, m[2], toTypeName(m[2]), m[2][3], m[2, 3], m[1, -1];
SELECT CAST('[[1,2],[3,4]]', 'FixedArray(2, 2, UInt8)') AS m, toTypeName(m);
SELECT defaultValueOfTypeName('FixedArray(2, 3, UInt8)');

SELECT '-- conversions';
SELECT [1, 2, 3]::FixedArray(3, UInt8)::Array(Float64) AS x, toTypeName(x);
SELECT [[1, 2], [3, 4]]::FixedArray(2, 2, UInt8)::Array(Array(String)) AS x, toTypeName(x);
SELECT [1, 2, 3]::FixedArray(3, UInt8)::FixedArray(3, Float32) AS x, toTypeName(x);
SELECT [1, 2, 3]::FixedArray(3, UInt8)::String;
SELECT [1, 2]::FixedArray(3, UInt8); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH, TYPE_MISMATCH }
SELECT materialize([1, 2])::FixedArray(3, UInt8); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT [1, 2, 3]::FixedArray(3, UInt8)::FixedArray(2, UInt8); -- { serverError TYPE_MISMATCH }
SELECT CAST('[1,2]', 'FixedArray(3, UInt8)'); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT [1]::FixedArray(0, UInt8); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT [1]::FixedArray(1000000, 1000000, UInt8); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT NULL::Nullable(FixedArray(1, UInt8)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- functions of arrays';
SELECT length(x), arraySum(x), has(x, 2), arrayFilter(v -> v > 1, x), arrayReverse(x), toTypeName(arrayReverse(x)), x = [1, 2, 3], x < [1, 2, 4]
FROM (SELECT [1, 2, 3]::FixedArray(3, UInt8) AS x);
SELECT arrayMap(v -> v * 10, x) AS y, toTypeName(y), arrayMap(r -> arraySum(r), m) AS s, toTypeName(s), arrayMap(r -> r, m) AS mm, toTypeName(mm)
FROM (SELECT [1, 2, 3]::FixedArray(3, UInt8) AS x, [[1, 2], [3, 4]]::FixedArray(2, 2, UInt8) AS m);
SELECT arrayMap((a, b) -> a + b, x, x) AS y, toTypeName(y), arrayMap((a, b) -> a + b, x, [1, 1, 1]) AS z, toTypeName(z)
FROM (SELECT [1, 2, 3]::FixedArray(3, UInt8) AS x);
SELECT arrayMap((a, b) -> a + b, [1, 2]::FixedArray(2, UInt8), [1, 2, 3]::FixedArray(3, UInt8)); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT arrayFlatten(m), toTypeName(arrayFlatten(m)) FROM (SELECT [[1, 2], [3, 4]]::FixedArray(2, 2, UInt8) AS m);
SELECT if(number = 0, [1, 2]::FixedArray(2, UInt8), [3, 4]::FixedArray(2, UInt8)) AS x, toTypeName(x) FROM numbers(2);
SELECT if(number = 0, [1, 2]::FixedArray(2, UInt8), [3, 4, 5]::FixedArray(3, UInt8)) AS x, toTypeName(x) FROM numbers(2);
SELECT [[1, 2]::FixedArray(2, UInt8), [3, 4]::FixedArray(2, UInt8)] AS x, toTypeName(x);
SELECT tuple([1, 2]::FixedArray(2, UInt8)) AS x, toTypeName(x), toTypeName(x.1);
SELECT x FROM (SELECT [1, 2]::FixedArray(2, UInt8) AS x) ARRAY JOIN x;
SELECT r, toTypeName(r) FROM (SELECT [[1, 2], [3, 4]]::FixedArray(2, 2, UInt8) AS m) ARRAY JOIN m AS r;
SELECT arrayJoin([[1, 2], [3, 4]]::FixedArray(2, 2, UInt8)) AS r, toTypeName(r);

SELECT '-- table';
DROP TABLE IF EXISTS t_fixed_array;
CREATE TABLE t_fixed_array (id UInt64, v FixedArray(4, Float32), m FixedArray(2, 3, UInt8), s FixedArray(2, String), n FixedArray(2, Nullable(Int32)))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_fixed_array VALUES (1, [1, 2, 3, 4], [[1, 2, 3], [4, 5, 6]], ['a', 'b'], [1, NULL]), (2, [5, 6, 7, 8], [[7, 8, 9], [10, 11, 12]], ['c', 'dd'], [NULL, 2]);
INSERT INTO t_fixed_array VALUES (3, [1, 2, 3], [[1, 2, 3], [4, 5, 6]], ['a', 'b'], [1, 2]); -- { error TYPE_MISMATCH }
INSERT INTO t_fixed_array SELECT number + 10, [number, number, number, number], [[1, 2, 3], [4, 5, 6]], ['x', toString(number)], [number, NULL] FROM numbers(3);
SELECT * FROM t_fixed_array ORDER BY id;
SELECT id, v[2], m[2], m[2, 1], s[2], n[1] FROM t_fixed_array ORDER BY id;
SELECT m, count() FROM t_fixed_array GROUP BY m ORDER BY m;
SELECT v FROM t_fixed_array ORDER BY v DESC LIMIT 2;
SELECT id FROM t_fixed_array WHERE m = [[7, 8, 9], [10, 11, 12]];
SELECT n.null FROM t_fixed_array ORDER BY id;
OPTIMIZE TABLE t_fixed_array FINAL;
SELECT sum(arraySum(v)), sum(length(m)) FROM t_fixed_array;
ALTER TABLE t_fixed_array MODIFY COLUMN v Array(Float64);
SELECT toTypeName(v), v FROM t_fixed_array ORDER BY id LIMIT 1;
ALTER TABLE t_fixed_array MODIFY COLUMN v FixedArray(4, Float64);
SELECT toTypeName(v), v FROM t_fixed_array ORDER BY id LIMIT 1;
DROP TABLE t_fixed_array;

SELECT '-- the size of a part: no offsets are stored';
DROP TABLE IF EXISTS t_fixed_array_size;
CREATE TABLE t_fixed_array_size (m FixedArray(100, 3, UInt8)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_fixed_array_size SELECT arrayMap(i -> [1, 2, 3], range(100)) FROM numbers(1000);
SELECT column, type, sum(data_uncompressed_bytes) FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_fixed_array_size' AND active GROUP BY column, type;
DROP TABLE t_fixed_array_size;

SELECT '-- formats';
SELECT [[1, 2], [3, 4]]::FixedArray(2, 2, UInt8) AS m, ['a', 'b']::FixedArray(2, String) AS s FORMAT JSONEachRow;
SELECT [[1, 2], [3, 4]]::FixedArray(2, 2, UInt8) AS m FORMAT CSV;
SELECT * FROM format(JSONEachRow, 'm FixedArray(2, 2, UInt8)', '{"m": [[1, 2], [3, 4]]}');
SELECT * FROM format(CSV, 'm FixedArray(2, UInt8)', '"[1,2]"');
SELECT * FROM format(JSONEachRow, 'm FixedArray(2, 2, UInt8)', '{"m": [[1, 2], [3]]}'); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT hex(x) FROM (SELECT * FROM format(RowBinary, 'x FixedArray(3, UInt8)', unhex('010203'))) FORMAT TSVRaw;
SELECT x FROM format(RowBinary, 'x FixedArray(3, UInt8)', unhex('010203'));
SELECT x::FixedArray(2, 2, UInt8) AS m, toTypeName(m) FROM format(Native, 'x Array(Array(UInt8))', (SELECT * FROM (SELECT [[1, 2], [3, 4]]) FORMAT Native));
SELECT CAST([1, 2]::FixedArray(2, UInt8), 'Dynamic') AS d, dynamicType(d);
