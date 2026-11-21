-- Aggregate functions with a Nullable argument under GROUP BY, over blocks of 1000 rows with and without NULLs.
-- Only block 20 (rows 20000-20999) contains NULLs. Key 7 is NULL in every row and occurs only in block 20,
-- key 8 occurs only in block 0, key 9 only in block 30.

SET compile_aggregate_expressions = 0;
SET optimize_injective_functions_in_group_by = 0;

DROP VIEW IF EXISTS tab;
CREATE VIEW tab AS
SELECT
    multiIf(number < 1000 AND number % 2 = 0, 8,
            number BETWEEN 20000 AND 20999 AND number % 10 = 0, 7,
            number BETWEEN 30000 AND 30999 AND number % 3 = 0, 9,
            number % 7) AS k,
    if(number BETWEEN 20000 AND 20999 AND (number % 7 = 0 OR number % 10 = 0), NULL, number + 1) AS v
FROM numbers(40000);

-- A UInt8 key
SELECT toUInt8(k) AS key, sum(v), avg(v), min(v), uniqExact(v), sumCount(v), sumOrNull(v)
FROM tab GROUP BY key ORDER BY key
SETTINGS max_block_size = 1000;

-- A String key
SELECT toString(k) AS key, sum(v), avg(v), min(v), uniqExact(v), sumCount(v), sum(toLowCardinality(v)),
    finalizeAggregation(sumState(v)), sum(toDecimal64(v, 2)), avg(toDecimal64(v, 2))
FROM tab GROUP BY key ORDER BY key
SETTINGS max_block_size = 1000;

-- The rows of the keys that do not fit into max_rows_to_group_by are skipped
SELECT toString(k) AS key, sum(v), avg(v), uniqExact(v)
FROM tab GROUP BY key ORDER BY key
SETTINGS max_block_size = 1000, max_threads = 1, max_rows_to_group_by = 4, group_by_overflow_mode = 'any',
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, enable_adaptive_aggregator = 0;

-- One state per row
SELECT count(), countIf(isNull(s)), sum(s)
FROM (SELECT initializeAggregation('sum', v) AS s FROM tab)
SETTINGS max_block_size = 1000;

-- Every state receives its rows in order, also with a UInt8 key: deltaSum of an increasing value is max - min
SELECT toUInt8(k) AS key, deltaSum(toUInt32(v)), max(v) - min(v)
FROM tab GROUP BY key ORDER BY key
SETTINGS max_block_size = 1000, max_threads = 1;

DROP VIEW tab;
