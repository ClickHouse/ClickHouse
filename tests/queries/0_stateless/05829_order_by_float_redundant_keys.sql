-- Tags: shard
-- `-0.0` and `0.0`, and the `NaN` payloads, compare equal, but functions can tell them apart.
-- So a function of a float sort key is not redundant after the key.

SET optimize_redundant_functions_in_order_by = 1;

SELECT count() FROM (SELECT x FROM values('x Float64', 0.0, -0.0) ORDER BY x, reinterpretAsUInt64(x) LIMIT 1 WITH TIES);
SELECT count() FROM (SELECT x FROM values('x Float32', 0.0, -0.0) ORDER BY x DESC, reinterpretAsUInt32(x) LIMIT 1 WITH TIES);
SELECT count() FROM (SELECT f FROM (SELECT reinterpretAsFloat64(x) AS f FROM values('x UInt64', 0x7FF8000000000000, 0x7FF8000000000001)) ORDER BY f, reinterpretAsUInt64(f) LIMIT 1 WITH TIES);
SELECT count() FROM (SELECT x FROM values('x Nullable(Float64)', 0.0, -0.0) ORDER BY x, reinterpretAsUInt64(assumeNotNull(x)) LIMIT 1 WITH TIES);
SELECT count() FROM (SELECT x FROM values('x Array(Float64)', [0.0], [-0.0]) ORDER BY x, toString(x) LIMIT 1 WITH TIES);
SELECT count() FROM (SELECT x FROM values('x Tuple(UInt8, Float64)', (1, 0.0), (1, -0.0)) ORDER BY x, toString(x) LIMIT 1 WITH TIES);

-- A function of a key that compares by value is still removed.
SELECT countIf(explain LIKE '%negate%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT x FROM values('x Int64', 1, 2) ORDER BY x, negate(x));
SELECT countIf(explain LIKE '%reinterpretAsUInt64%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT x FROM values('x Float64', 0.0, -0.0) ORDER BY x, reinterpretAsUInt64(x));

-- A kept positional key loses the aliases that the projection loses, so a remote server does not find two definitions of them.
SET enable_positional_arguments = 1;
SELECT count() FROM (SELECT n, negate(abs(n) AS a) AS m FROM remote('127.0.0.2', values('n Float64', 0.0, -0.0)) ORDER BY n, 2);
SELECT count() FROM (SELECT n, negate(abs(n) AS a) AS m FROM remote('127.0.0.2', values('n Float64', 0.0, -0.0)) GROUP BY n WITH ROLLUP ORDER BY n, 2);
SELECT count() FROM (SELECT n, negate(abs(n) AS a) AS m FROM remote('127.0.0.{2,3}', values('n Float64', 0.0, -0.0)) ORDER BY n, 2 LIMIT 1 WITH TIES);
