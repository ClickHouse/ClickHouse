DROP TABLE IF EXISTS squality_case_mode_cache;
CREATE TABLE squality_case_mode_cache (x Nullable(Int8), y Nullable(Int8))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO squality_case_mode_cache VALUES (NULL, NULL);

SELECT count() FROM squality_case_mode_cache
WHERE caseWithExpression(x, y, 1, 0)
SETTINGS use_query_condition_cache = 1, optimize_move_to_prewhere = 0,
    case_with_expression_null_equals_null = 0;
SELECT count() FROM squality_case_mode_cache
WHERE caseWithExpression(x, y, 1, 0)
SETTINGS use_query_condition_cache = 1, optimize_move_to_prewhere = 0,
    case_with_expression_null_equals_null = 1;

DROP TABLE squality_case_mode_cache;

SET allow_experimental_variant_type = 1;
SELECT CASE CAST(NULL AS Variant(Int8, String))
    WHEN CAST(NULL AS Variant(Int8, String)) THEN 'match' ELSE 'default' END
SETTINGS case_with_expression_null_equals_null = 0,
    use_variant_default_implementation_for_comparisons = 0,
    allow_experimental_variant_type = 1;

DROP TABLE IF EXISTS squality_case_variant_nulls;
CREATE TABLE squality_case_variant_nulls
(x Variant(Int8, String), y Variant(Int8, String)) ENGINE = Memory;
INSERT INTO squality_case_variant_nulls VALUES (NULL, NULL);
SELECT CASE x WHEN y THEN 'match' ELSE 'default' END
FROM squality_case_variant_nulls
SETTINGS case_with_expression_null_equals_null = 0,
    use_variant_default_implementation_for_comparisons = 0,
    allow_experimental_variant_type = 1;
DROP TABLE squality_case_variant_nulls;
