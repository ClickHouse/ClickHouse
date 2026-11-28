-- Tags: no-parallel-replicas

-- A normal projection that does not cover the query and cannot be read through the projection index only drops
-- parts, so its primary-key analysis stops refining a part at the first match. The dropped parts and the result must not change.

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tp;

CREATE TABLE tp (k UInt64, v UInt64, PROJECTION p (SELECT v ORDER BY v))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO tp SELECT number, number FROM numbers(10000);
INSERT INTO tp SELECT number, number FROM numbers(10000, 10000);
INSERT INTO tp SELECT number, number FROM numbers(20000, 10000);

SET optimize_use_projections = 1, optimize_use_projection_filtering = 1, use_statistics_for_part_pruning = 0,
    use_query_condition_cache = 0, use_index_for_in_with_subqueries = 1, merge_tree_coarse_index_granularity = 8,
    merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0, merge_tree_generic_exclusion_search_max_steps = 0,
    enable_parallel_replicas = 0;

-- { echoOn }

-- Only the second part has a matching row.
SELECT count(), sum(k) FROM tp WHERE v IN (SELECT 10000 + number * 50 FROM numbers(200));
SELECT count(), sum(k) FROM tp WHERE v IN (SELECT 10000 + number * 50 FROM numbers(200)) SETTINGS optimize_use_projection_filtering = 0;
SELECT trimLeft(explain) FROM (EXPLAIN projections = 1 SELECT count(), sum(k) FROM tp WHERE v IN (SELECT 10000 + number * 50 FROM numbers(200)))
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

-- The first and the third parts.
SELECT count(), sum(k) FROM tp WHERE v IN (SELECT number * 50 FROM numbers(200)) OR v IN (SELECT 20000 + number * 50 FROM numbers(200));
SELECT count(), sum(k) FROM tp WHERE v IN (SELECT number * 50 FROM numbers(200)) OR v IN (SELECT 20000 + number * 50 FROM numbers(200)) SETTINGS optimize_use_projection_filtering = 0;
SELECT trimLeft(explain) FROM (EXPLAIN projections = 1 SELECT count(), sum(k) FROM tp WHERE v IN (SELECT number * 50 FROM numbers(200)) OR v IN (SELECT 20000 + number * 50 FROM numbers(200)))
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

-- No part.
SELECT count(), sum(k) FROM tp WHERE v IN (SELECT 100000 + number FROM numbers(10));
SELECT trimLeft(explain) FROM (EXPLAIN projections = 1 SELECT count(), sum(k) FROM tp WHERE v IN (SELECT 100000 + number FROM numbers(10)))
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

-- { echoOff }

DROP TABLE tp;
