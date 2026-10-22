-- Each build-side stream of a join builds its own partial runtime filter, and the partials are
-- merged. When every partial has turned into a bloom filter, the merge ORs their bits, so the two
-- partials must use the same layout and hashing: a probe key contributed by either of them passes.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SET join_runtime_filter_exact_values_limit = 1; -- every partial turns into a bloom filter
SET join_runtime_filter_from_fixed_hash_table = 0; -- keep the merged bloom filter as the runtime filter
SET enable_join_runtime_filters_index_analysis = 0;
SET join_runtime_bloom_filter_max_ratio_of_set_bits = 1;
SET query_plan_join_swap_table = 0;
SET join_algorithm = 'hash';
SET max_threads = 2;

DROP TABLE IF EXISTS fact;

CREATE TABLE fact (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO fact SELECT number, toString(number) FROM numbers(100000);

-- The build side is a union of two halves of the key set, read by two streams.
SELECT explain FROM (EXPLAIN PIPELINE
    SELECT count() FROM fact
    INNER JOIN (SELECT number * 7 AS k FROM numbers(50) UNION ALL SELECT number * 7 + 350 AS k FROM numbers(50)) AS dim
    ON fact.k = dim.k)
WHERE explain LIKE '%BuildRuntimeFilterTransform%';

SELECT '-- UInt64 key';
SELECT count(), countIf(fact.k < 350), countIf(fact.k >= 350) FROM fact
INNER JOIN (SELECT number * 7 AS k FROM numbers(50) UNION ALL SELECT number * 7 + 350 AS k FROM numbers(50)) AS dim
ON fact.k = dim.k;
SELECT count(), countIf(fact.k < 350), countIf(fact.k >= 350) FROM fact
INNER JOIN (SELECT number * 7 AS k FROM numbers(50) UNION ALL SELECT number * 7 + 350 AS k FROM numbers(50)) AS dim
ON fact.k = dim.k
SETTINGS join_runtime_bloom_filter_hash_functions = 10;

SELECT '-- UInt32 key';
SELECT count(), countIf(fact.k < 350), countIf(fact.k >= 350) FROM fact
INNER JOIN (SELECT toUInt32(number * 7) AS k FROM numbers(50) UNION ALL SELECT toUInt32(number * 7 + 350) AS k FROM numbers(50)) AS dim
ON toUInt32(fact.k) = dim.k;

SELECT '-- String key';
SELECT count(), countIf(fact.k < 350), countIf(fact.k >= 350) FROM fact
INNER JOIN (SELECT toString(number * 7) AS s FROM numbers(50) UNION ALL SELECT toString(number * 7 + 350) AS s FROM numbers(50)) AS dim
ON fact.s = dim.s;

DROP TABLE fact;
