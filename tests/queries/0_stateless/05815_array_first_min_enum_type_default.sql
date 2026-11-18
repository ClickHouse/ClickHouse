-- arrayFirst/arrayLast without a match and arrayMin/arrayMax of an empty array return the default of the element type.
-- For an Enum it is the lowest member, not the column zero.

DROP TABLE IF EXISTS t_arr;
DROP TABLE IF EXISTS t_res;

CREATE TABLE t_arr (id UInt8, arr Array(Enum8('x' = 1, 'y' = 2))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_arr VALUES (1, ['y']), (2, []), (3, ['y', 'x']);

SELECT id, arrayFirst(x -> x = 'y', arr), arrayFirst(x -> x > 'y', arr), arrayLast(x -> x > 'y', arr), arrayMin(arr), arrayMax(x -> x, arr), arrayMax(x -> CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)'), arr)
FROM t_arr ORDER BY id;

SELECT id, arrayFirst(x -> 1, arr), arrayLast(x -> 1, arr) FROM t_arr ORDER BY id;

CREATE TABLE t_res (id UInt8, e Enum8('x' = 1, 'y' = 2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_res SELECT id, arrayFirst(x -> x > 'y', arr) FROM t_arr;
INSERT INTO t_res SELECT id + 10, arrayMax(arr) FROM t_arr;
SELECT * FROM t_res ORDER BY id;

SELECT arrayFirst(x -> 0, [CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')]), arrayLast(x -> 0, materialize([CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')]));
SELECT arrayFirst(x -> 1, materialize(CAST([], 'Array(Enum8(\'x\' = 1, \'y\' = 2))'))), arrayLast(x -> 1, materialize(CAST([], 'Array(Enum8(\'x\' = 1, \'y\' = 2))')));
SELECT arrayLast(x -> x = 'x', materialize([CAST('y', 'Enum16(\'x\' = 1000, \'y\' = 2000)')])), arrayMin(materialize(CAST([], 'Array(Enum16(\'x\' = 1000, \'y\' = 2000))')));

SELECT
    arrayFirst(x -> 0, [CAST('y', 'Enum8(\'a\' = -1, \'b\' = 0, \'y\' = 2)')]),
    arrayLast(x -> x = 'a', materialize([CAST('y', 'Enum8(\'a\' = -1, \'b\' = 0, \'y\' = 2)')])),
    arrayMin(CAST([], 'Array(Enum8(\'a\' = -1, \'b\' = 0, \'y\' = 2))')),
    arrayMax(x -> CAST('y', 'Enum8(\'a\' = -1, \'b\' = 0, \'y\' = 2)'), materialize(CAST([], 'Array(UInt8)'))),
    defaultValueOfTypeName('Enum8(\'a\' = -1, \'b\' = 0, \'y\' = 2)');

SELECT arrayFirst((e, n) -> n = 9, CAST([('y', 1)], 'Array(Tuple(e Enum8(\'x\' = 1, \'y\' = 2), n UInt8))')), arrayMin(CAST([], 'Array(Tuple(e Enum8(\'x\' = 1, \'y\' = 2), n UInt8))'));
SELECT arrayFirst(x -> 0, [CAST('{"a":"y"}', 'JSON(a Enum8(\'x\' = 1, \'y\' = 2))')]), arrayMax(CAST([], 'Array(JSON(a Enum8(\'x\' = 1, \'y\' = 2)))'));

-- Unchanged: types whose default is the column zero, Nullable elements, the OrNull variants, the other aggregations.
SELECT arrayFirst(x -> 0, [1::UInt8]), arrayLast(x -> 0, ['s']), arrayMin(CAST([], 'Array(String)')), arrayFirst(x -> 0, [CAST('y', 'Nullable(Enum8(\'x\' = 1, \'y\' = 2))')]), arrayMin(CAST([], 'Array(Nullable(Enum8(\'x\' = 1, \'y\' = 2)))')), arrayFirstOrNull(x -> 0, [CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')]), arraySum(CAST([], 'Array(UInt8)')), arrayAvg(CAST([], 'Array(Enum8(\'x\' = 1, \'y\' = 2))'));
SELECT dumpColumnStructure(arrayFirst(x -> 1, materialize([]))), dumpColumnStructure(arrayLast(x -> 0, materialize([]))), dumpColumnStructure(arrayMin(materialize([]))), dumpColumnStructure(arrayMax(x -> x, materialize([])));

DROP TABLE t_arr;
DROP TABLE t_res;
