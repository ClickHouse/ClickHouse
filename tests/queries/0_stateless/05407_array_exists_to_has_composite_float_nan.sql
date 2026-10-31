-- `arrayExists(x -> x = c, arr)` must not be rewritten to `has(arr, c)` when the element is a
-- composite carrier of floats: `has` compares such values with the generic column comparison,
-- which treats two NaNs as equal, while `equals` compares the nested floats numerically.

DROP TABLE IF EXISTS t_variant_float;
CREATE TABLE t_variant_float (v Variant(Float64)) ENGINE = Memory;
INSERT INTO t_variant_float SELECT CAST(nan, 'Variant(Float64)');

SELECT v = CAST(nan, 'Variant(Float64)'), arrayExists(x -> x = CAST(nan, 'Variant(Float64)'), [v])
FROM t_variant_float SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT v = CAST(nan, 'Variant(Float64)'), arrayExists(x -> x = CAST(nan, 'Variant(Float64)'), [v])
FROM t_variant_float SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT count() FROM t_variant_float WHERE arrayExists(x -> x = CAST(nan, 'Variant(Float64)'), [v]) SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT count() FROM t_variant_float WHERE arrayExists(x -> x = CAST(nan, 'Variant(Float64)'), [v]) SETTINGS optimize_rewrite_array_exists_to_has = 0;

DROP TABLE t_variant_float;

DROP TABLE IF EXISTS t_tuple_float;
CREATE TABLE t_tuple_float (arr Array(Tuple(Float64, UInt8))) ENGINE = Memory;
INSERT INTO t_tuple_float VALUES ([(nan, 1)]);

SELECT arrayExists(x -> x = (nan, 1), arr) FROM t_tuple_float SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = (nan, 1), arr) FROM t_tuple_float SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT count() FROM t_tuple_float WHERE arrayExists(x -> x = (nan, 1), arr) SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT count() FROM t_tuple_float WHERE arrayExists(x -> x = (nan, 1), arr) SETTINGS optimize_rewrite_array_exists_to_has = 0;

-- A plain float element is still rewritten: both spellings compare it numerically.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = 1.5, [1.5, nan]) SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain LIKE '%function_name: has%';
SELECT arrayExists(x -> x = nan, [1.5, nan]) SETTINGS optimize_rewrite_array_exists_to_has = 1;

DROP TABLE t_tuple_float;
