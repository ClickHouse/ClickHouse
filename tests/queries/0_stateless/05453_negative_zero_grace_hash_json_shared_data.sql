-- `grace_hash` join partitions the keys by `computeHashInto` before building the per-bucket hash tables.
-- For `JSON(max_dynamic_paths=0)` every path is in the shared data, and its values were hashed as their binary encoding,
-- so `-0.` and `0.` went to different buckets and were never matched, while `hash` join matched them.

SET allow_dynamic_type_in_join_keys = 1;
SET grace_hash_join_initial_buckets = 16;
SET max_threads = 8;

SELECT 'JSON, grace_hash';
SELECT count()
FROM (SELECT materialize(CAST('{"a":0.0}', 'JSON(max_dynamic_paths=0)')) AS x) AS l
INNER JOIN (SELECT materialize(CAST('{"a":-0.0}', 'JSON(max_dynamic_paths=0)')) AS x) AS r
USING x
SETTINGS join_algorithm = 'grace_hash';

SELECT 'JSON, hash';
SELECT count()
FROM (SELECT materialize(CAST('{"a":0.0}', 'JSON(max_dynamic_paths=0)')) AS x) AS l
INNER JOIN (SELECT materialize(CAST('{"a":-0.0}', 'JSON(max_dynamic_paths=0)')) AS x) AS r
USING x
SETTINGS join_algorithm = 'hash';

SELECT 'Array(JSON)';
SELECT count()
FROM (SELECT materialize([CAST('{"a":0.0}', 'JSON(max_dynamic_paths=0)')]) AS x) AS l
INNER JOIN (SELECT materialize([CAST('{"a":-0.0}', 'JSON(max_dynamic_paths=0)')]) AS x) AS r
USING x
SETTINGS join_algorithm = 'grace_hash';

SELECT 'Tuple(JSON, UInt8)';
SELECT count()
FROM (SELECT materialize(tuple(CAST('{"a":0.0}', 'JSON(max_dynamic_paths=0)'), 1)) AS x) AS l
INNER JOIN (SELECT materialize(tuple(CAST('{"a":-0.0}', 'JSON(max_dynamic_paths=0)'), 1)) AS x) AS r
USING x
SETTINGS join_algorithm = 'grace_hash';

SELECT 'Negative zero inside an array value of a path';
SELECT count()
FROM (SELECT materialize(CAST('{"a":[1.5,0.0]}', 'JSON(max_dynamic_paths=0)')) AS x) AS l
INNER JOIN (SELECT materialize(CAST('{"a":[1.5,-0.0]}', 'JSON(max_dynamic_paths=0)')) AS x) AS r
USING x
SETTINGS join_algorithm = 'grace_hash';

-- Many rows, with other paths in the shared data before and after the zero, so that most of the rows land in a wrong bucket without the fix.
SELECT 'Many rows';
SELECT count(), countIf(l.n = r.n)
FROM (SELECT number AS n, CAST(concat('{"k":', toString(number), ',"s":"x","z":0.0}'), 'JSON(max_dynamic_paths=0)') AS x FROM numbers(100)) AS l
INNER JOIN (SELECT number AS n, CAST(concat('{"k":', toString(number), ',"s":"x","z":-0.0}'), 'JSON(max_dynamic_paths=0)') AS x FROM numbers(100)) AS r
USING x
SETTINGS join_algorithm = 'grace_hash';

-- The same with dynamic paths, which were already hashed by their decoded values.
SELECT 'Many rows, dynamic paths';
SELECT count(), countIf(l.n = r.n)
FROM (SELECT number AS n, CAST(concat('{"k":', toString(number), ',"s":"x","z":0.0}'), 'JSON') AS x FROM numbers(100)) AS l
INNER JOIN (SELECT number AS n, CAST(concat('{"k":', toString(number), ',"s":"x","z":-0.0}'), 'JSON') AS x FROM numbers(100)) AS r
USING x
SETTINGS join_algorithm = 'grace_hash';
