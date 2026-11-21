-- a merge of projection parts sizes the granules by the columns that the parts store, not by the subcolumns that the projection names
-- the twin tables have the projection materialized and show the read that the optimizer selects
-- fixed merge block size and batch sorting queue: tiny random merge blocks make the real projection merges slow under the thread fuzzer
-- no vertical merge: it keeps the granules of the merged parts, the layout of their inserts

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

SELECT '-- wide';
CREATE TABLE w_est (a UInt64, b UInt64, t Tuple(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, merge_use_batch_sorting_queue = 1, use_const_adaptive_granularity = 0,
        ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '', enable_vertical_merge_algorithm = 0;
CREATE TABLE w_real AS w_est;
ALTER TABLE w_real ADD PROJECTION p_s (SELECT a, b, t, t.y ORDER BY b);
INSERT INTO w_est SELECT number, number % 100, (number, number) FROM numbers(1500);
INSERT INTO w_est SELECT number + 1500, number % 100, (number, number) FROM numbers(1500);
INSERT INTO w_real SELECT number, number % 100, (number, number) FROM numbers(1500);
INSERT INTO w_real SELECT number + 1500, number % 100, (number, number) FROM numbers(1500);
OPTIMIZE TABLE w_est FINAL;
OPTIMIZE TABLE w_real FINAL;

CREATE HYPOTHETICAL PROJECTION p_s ON w_est (SELECT a, b, t, t.y ORDER BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, t FROM w_est WHERE b < 50 AND a < 1700)
WHERE match(line, '^(With |marks:|verdict:)');
SELECT if(countIf(explain LIKE '%ReadFromMergeTree (p_s)%') > 0, 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, t FROM w_real WHERE b < 50 AND a < 1700);
SELECT 'real forced: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, t FROM w_real WHERE b < 50 AND a < 1700 SETTINGS force_optimize_projection = 1)
WHERE match(trim(explain), '^Granules:');

SELECT '-- compact';
CREATE TABLE c_est (a UInt64, b UInt64, t Tuple(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = '1Gi', min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, merge_use_batch_sorting_queue = 1, use_const_adaptive_granularity = 0,
        ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '', enable_vertical_merge_algorithm = 0;
CREATE TABLE c_real AS c_est;
ALTER TABLE c_real ADD PROJECTION p_s (SELECT a, b, t, t.y ORDER BY b);
INSERT INTO c_est SELECT number, number % 100, (number, number) FROM numbers(1500);
INSERT INTO c_est SELECT number + 1500, number % 100, (number, number) FROM numbers(1500);
INSERT INTO c_real SELECT number, number % 100, (number, number) FROM numbers(1500);
INSERT INTO c_real SELECT number + 1500, number % 100, (number, number) FROM numbers(1500);
OPTIMIZE TABLE c_est FINAL;
OPTIMIZE TABLE c_real FINAL;

CREATE HYPOTHETICAL PROJECTION p_s ON c_est (SELECT a, b, t, t.y ORDER BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, t FROM c_est WHERE b < 50 AND a < 1700)
WHERE match(line, '^(With |marks:|verdict:)');
SELECT if(countIf(explain LIKE '%ReadFromMergeTree (p_s)%') > 0, 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, t FROM c_real WHERE b < 50 AND a < 1700);
SELECT 'real forced: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, t FROM c_real WHERE b < 50 AND a < 1700 SETTINGS force_optimize_projection = 1)
WHERE match(trim(explain), '^Granules:');

DROP TABLE w_est;
DROP TABLE w_real;
DROP TABLE c_est;
DROP TABLE c_real;
