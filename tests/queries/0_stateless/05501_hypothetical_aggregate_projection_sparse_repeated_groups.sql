-- under a constant granularity a merge sizes the granules from the bytes per row of the parts it merges
-- a group with a sparse default key that is in more of these parts makes that average smaller than the average over the groups
-- the twin table has the projection materialized and shows its marks and the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

-- the implicit minmax indices of add_minmax_index_for_numeric_columns would prune the marks of the base table and change
-- the estimates and the real reads this test compares, so the tables opt out
CREATE TABLE r_est (a UInt8, b UInt16, v UInt8) ENGINE = MergeTree ORDER BY b
    SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 280, index_granularity_bytes = 10485760, use_const_adaptive_granularity = 1, min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0, merge_max_block_size = 8192, enable_vertical_merge_algorithm = 0, enable_block_number_column = 0,
        enable_block_offset_column = 0, ratio_of_defaults_for_sparse_serialization = 0.9375,
        max_bytes_to_merge_at_max_space_in_pool = 1;
CREATE TABLE r_real AS r_est;
ALTER TABLE r_real ADD PROJECTION p (SELECT toUInt64(a) AS A, toUInt64(b) AS B, sum(v) GROUP BY A, B)
    WITH SETTINGS (index_granularity = 8192, index_granularity_bytes = 1024);
-- only OPTIMIZE merges, and it merges all parts at once
-- 400 groups with the key `A = 1` in one part, and the same 100 groups with the default key `A = 0` in 20 parts
INSERT INTO r_est SELECT 1, number + 1, 1 FROM numbers(400) SETTINGS max_block_size = 65536, min_insert_block_size_rows = 65536;
INSERT INTO r_real SELECT 1, number + 1, 1 FROM numbers(400) SETTINGS max_block_size = 65536, min_insert_block_size_rows = 65536;
INSERT INTO r_est SELECT 0, number % 100 + 1, 1 FROM numbers(2000) SETTINGS max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO r_real SELECT 0, number % 100 + 1, 1 FROM numbers(2000) SETTINGS max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, max_insert_threads = 1;
SELECT 'parts before the merge: ' || toString(count()) FROM system.parts WHERE database = currentDatabase() AND table = 'r_real' AND active;
OPTIMIZE TABLE r_est FINAL;
OPTIMIZE TABLE r_real FINAL;

CREATE HYPOTHETICAL PROJECTION p ON r_est (SELECT toUInt64(a) AS A, toUInt64(b) AS B, sum(v) GROUP BY A, B)
    WITH SETTINGS (index_granularity = 8192, index_granularity_bytes = 1024);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT toUInt64(a) AS A, toUInt64(b) AS B, sum(v) FROM r_est GROUP BY A, B)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT 'real projection part: ' || part_type || ', ' || toString(rows) || ' rows, ' || toString(marks - 1) || ' marks'
FROM system.projection_parts WHERE database = currentDatabase() AND table = 'r_real' AND name = 'p' AND active;
SELECT 'real: ' || toString(marks) || ' marks'
FROM (EXPLAIN ESTIMATE SELECT toUInt64(a) AS A, toUInt64(b) AS B, sum(v) FROM r_real GROUP BY A, B);

DROP HYPOTHETICAL PROJECTION p ON r_est;
DROP TABLE r_est;
DROP TABLE r_real;
