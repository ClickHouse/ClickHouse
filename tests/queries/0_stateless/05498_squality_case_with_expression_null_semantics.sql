SET case_with_expression_null_equals_null = true;

SELECT CASE CAST(NULL AS Nullable(Int8)) WHEN CAST(NULL AS Nullable(Int8)) THEN 'match' ELSE 'default' END;

SET case_with_expression_null_equals_null = false;

SELECT CASE CAST(NULL AS Nullable(Int8)) WHEN CAST(NULL AS Nullable(Int8)) THEN 'match' ELSE 'default' END;
SELECT CASE CAST(NULL AS Nullable(Float64)) WHEN CAST(NULL AS Nullable(Int64)) THEN -60 END + 81;

SELECT
    id,
    CASE x WHEN y THEN 'match' ELSE 'default' END
FROM values(
    'id UInt8, x Nullable(Int8), y Nullable(Int8)',
    (1, NULL, NULL),
    (2, 1, 1),
    (3, NULL, 1),
    (4, 1, NULL),
    (5, 1, 2))
ORDER BY id;

SELECT caseWithExpression(
    toLowCardinality(CAST(NULL AS Nullable(String))),
    toLowCardinality(CAST(NULL AS Nullable(String))),
    'match',
    'default');

SET allow_experimental_variant_type = 1;

SELECT CASE CAST(NULL AS Nullable(Int8)) WHEN CAST(NULL AS Variant(Int8, String)) THEN 'match' ELSE 'default' END;
SELECT CASE CAST(NULL AS Nullable(Int8)) WHEN CAST(NULL AS Variant(Int8, String)) THEN materialize('match') ELSE 'default' END;
SELECT CASE CAST(NULL AS Dynamic) WHEN CAST(NULL AS Dynamic) THEN 'match' ELSE 'default' END;
SELECT CASE tuple(CAST(NULL AS Nullable(Int8))) WHEN tuple(CAST(NULL AS Nullable(Int8))) THEN 'match' ELSE 'default' END;

SELECT CASE WHEN isNull(NULL) THEN 'match' ELSE 'default' END;

SET case_with_expression_null_equals_null = true;

SELECT caseWithExpression(
    toLowCardinality(CAST(NULL AS Nullable(String))),
    toLowCardinality(CAST(NULL AS Nullable(String))),
    'match',
    'default');
