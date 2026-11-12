DROP TABLE IF EXISTS squality_exact_numeric_condition_cache;
CREATE TABLE squality_exact_numeric_condition_cache
(
    d Decimal(18, 0),
    x Float64
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO squality_exact_numeric_condition_cache
SELECT toDecimal64('9007199254740993', 0), 9007199254740992. FROM numbers(10);

SELECT count() FROM squality_exact_numeric_condition_cache WHERE isNotDistinctFrom(d, x)
SETTINGS allow_lossy_numeric_supertype = 1, use_query_condition_cache = 1;
SELECT count() FROM squality_exact_numeric_condition_cache WHERE isNotDistinctFrom(d, x)
SETTINGS allow_lossy_numeric_supertype = 0, use_query_condition_cache = 0;
SELECT count() FROM squality_exact_numeric_condition_cache WHERE isNotDistinctFrom(d, x)
SETTINGS allow_lossy_numeric_supertype = 0, use_query_condition_cache = 1;

SELECT count() FROM squality_exact_numeric_condition_cache WHERE isDistinctFrom(d, x)
SETTINGS allow_lossy_numeric_supertype = 0, use_query_condition_cache = 1;
SELECT count() FROM squality_exact_numeric_condition_cache WHERE isDistinctFrom(d, x)
SETTINGS allow_lossy_numeric_supertype = 1, use_query_condition_cache = 0;
SELECT count() FROM squality_exact_numeric_condition_cache WHERE isDistinctFrom(d, x)
SETTINGS allow_lossy_numeric_supertype = 1, use_query_condition_cache = 1;

DROP TABLE squality_exact_numeric_condition_cache;
