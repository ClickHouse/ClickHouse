-- An `Enum` is ordered against a `FixedString` constant by its name, so the folds of the redundant comparisons,
-- which compare `Enum` ordinals, must not use such a bound.
DROP TABLE IF EXISTS e;
CREATE TABLE e (k Enum8('a' = 2, 'b' = 1)) ENGINE = Memory;
INSERT INTO e VALUES ('b');

SELECT count() FROM e
WHERE k > toFixedString('a', 2) AND k = CAST('b' AS Enum8('a' = 2, 'b' = 1))
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0, optimize_constant_columns_after_filter = 0;

SELECT count() FROM e
WHERE k > toFixedString('a', 2) AND k = CAST('b' AS Enum8('a' = 2, 'b' = 1))
SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0, optimize_constant_columns_after_filter = 0;

SELECT count() FROM e
WHERE k < toFixedString('a', 2) AND k = CAST('b' AS Enum8('a' = 2, 'b' = 1))
SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0, optimize_constant_columns_after_filter = 0;

DROP TABLE e;

-- A comparison of maps with `FixedString` values is not a join key, as for arrays.
DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS s;
CREATE TABLE f (k Map(String, FixedString(2))) ENGINE = Memory;
CREATE TABLE s (k Map(String, String)) ENGINE = Memory;
INSERT INTO f SELECT map('k', toFixedString('a', 2));
INSERT INTO s SELECT map('k', 'a\0');

SELECT count() FROM f CROSS JOIN s WHERE f.k = s.k
SETTINGS cross_to_inner_join_rewrite = 0, query_plan_merge_filter_into_join_condition = 0, optimize_constant_columns_after_filter = 0;

SELECT count() FROM f CROSS JOIN s WHERE f.k = s.k
SETTINGS cross_to_inner_join_rewrite = 1, query_plan_merge_filter_into_join_condition = 0, optimize_constant_columns_after_filter = 0;

SELECT count() FROM f JOIN s ON f.k = s.k;

SELECT count() FROM f JOIN s ON tuple(f.k) = tuple(s.k);

DROP TABLE f;
DROP TABLE s;
