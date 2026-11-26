-- Aggregate functions pad an empty result with the default of the result type; for an Enum it is the smallest member, not the column zero.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_res;

CREATE TABLE t (n UInt8, e Enum8('a' = 1, 'b' = 2)) ENGINE = Memory;
INSERT INTO t VALUES (1, 'b'), (3, 'b');

SELECT 'OrDefault';
SELECT anyOrDefault(e), minOrDefault(e), argMaxOrDefault(e, n), any(e) FROM t WHERE 0;
SELECT n, anyOrDefaultIf(e, n = 3) FROM t GROUP BY n ORDER BY n;
SELECT anyOrDefault(tuple(e)), anyOrDefault(CAST(toString(e), 'Enum16(\'a\' = 1000, \'b\' = 2000)')) FROM t WHERE 0;
SELECT finalizeAggregation(anyOrDefaultState(e)) FROM t WHERE 0;
SELECT anyOrDefault(j) FROM (SELECT '{"a": "b"}'::JSON(a Enum8('a' = 1, 'b' = 2)) AS j) WHERE 0;
SELECT anyOrNull(e), anyOrDefault(CAST(e, 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))')), sumOrDefault(n) FROM t WHERE 0;

SELECT 'minMap and maxMap';
SELECT minMap([1, 2], CAST([NULL, 'b'], 'Array(Nullable(Enum8(\'a\' = 1, \'b\' = 2)))')), maxMap([1, 2], CAST([NULL, 'b'], 'Array(Nullable(Enum8(\'a\' = 1, \'b\' = 2)))'));
SELECT minMap(([1, 2], CAST([NULL, 'b'], 'Array(Nullable(Enum8(\'a\' = 1, \'b\' = 2)))')));
SELECT minMap([1], CAST([NULL], 'Array(Nullable(Tuple(Enum8(\'a\' = 1, \'b\' = 2))))'));
SELECT minMap(k, v) FROM (SELECT [1] AS k, CAST([NULL], 'Array(Nullable(Enum8(\'a\' = 1, \'b\' = 2)))') AS v UNION ALL SELECT [1], CAST(['b'], 'Array(Nullable(Enum8(\'a\' = 1, \'b\' = 2)))'));
SELECT minMap([1], CAST([NULL], 'Array(Nullable(UInt8))'));
SELECT minMap(CAST(map(1, NULL, 2, 'b'), 'Map(UInt8, Nullable(Enum8(\'a\' = 1, \'b\' = 2)))'));

SELECT 'mergedJSONPatch';
SELECT mergedJSONPatch(j, 1) FROM (SELECT '{"a": "b"}'::JSON(a Enum8('a' = 1, 'b' = 2)) AS j) WHERE 0;
SELECT mergedJSONPatch(j, 1) FROM (SELECT '{"a": {"x": "b"}}'::JSON(a Tuple(x Enum8('a' = 1, 'b' = 2))) AS j) WHERE 0;
SELECT mergedJSONPatch(j, 1) FROM (SELECT '{"a": "b"}'::JSON(a Nullable(Enum8('a' = 1, 'b' = 2))) AS j) WHERE 0;

SELECT 'nothing';
SELECT nothing(e), nothing(tuple(e)) FROM t;
SELECT nothing(CAST(e, 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))')) FROM t;
SELECT nothingNull(e), nothingUInt64(e) FROM t;

SELECT 'Enum with a 0 member that is not the smallest';
SELECT anyOrDefault(x), nothing(x) FROM (SELECT CAST('n', 'Enum8(\'m\' = -1, \'n\' = 0)') AS x) WHERE 0;
SELECT nothing(x) FROM (SELECT CAST('n', 'Enum8(\'m\' = -1, \'n\' = 0)') AS x);
SELECT defaultValueOfTypeName('Enum8(\'m\' = -1, \'n\' = 0)');

SELECT 'stored';
CREATE TABLE t_res (k String, e Enum8('a' = 1, 'b' = 2)) ENGINE = Memory;
INSERT INTO t_res SELECT 'or_default', anyOrDefault(e) FROM t WHERE 0;
INSERT INTO t_res SELECT 'nothing', nothing(e) FROM t;
SELECT * FROM t_res ORDER BY k;

SELECT 'Nothing';
SELECT dumpColumnStructure(nothing(arrayFirst(x -> 1, materialize([])))) FROM numbers(1) WHERE 0;
SELECT dumpColumnStructure(anyOrDefault(arrayFirst(x -> 1, materialize([])))) FROM numbers(1) WHERE 0;
SELECT dumpColumnStructure(nothing(arrayFirst(x -> 1, materialize([])))) FROM numbers(1);
SELECT tupleElement(anyOrDefault(tuple(e, arrayFirst(x -> 1, materialize([])))), 1) FROM t WHERE 0;
SELECT tupleElement(nothing(tuple(e, arrayFirst(x -> 1, materialize([])))), 1) FROM t;
SELECT count() FROM (SELECT anyOrDefault(tuple(e, arrayFirst(x -> 1, materialize([])))) FROM t WHERE 0);

DROP TABLE t;
DROP TABLE t_res;
