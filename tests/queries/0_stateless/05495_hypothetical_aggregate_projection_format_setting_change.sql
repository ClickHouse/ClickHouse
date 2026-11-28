-- a merge keeps the wide format of a part that it merges, so a wide part written before a change of min_rows_for_wide_part stays wide
-- the twin table has the projection materialized and shows its format, its marks and the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

CREATE TABLE f_est (b UInt64, v UInt64) ENGINE = MergeTree ORDER BY v
    SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, use_const_adaptive_granularity = 1, min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0, merge_max_block_size = 8192, enable_vertical_merge_algorithm = 0, enable_block_number_column = 0,
        enable_block_offset_column = 0, ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE f_real AS f_est;
ALTER TABLE f_real ADD PROJECTION p (SELECT b, sum(v) GROUP BY b);
-- 300 groups in each wide part, 100 of them in both: 600 rows merge into 500 groups
INSERT INTO f_est SELECT number, number FROM numbers(300);
INSERT INTO f_est SELECT number + 200, number + 300 FROM numbers(300);
INSERT INTO f_real SELECT number, number FROM numbers(300);
INSERT INTO f_real SELECT number + 200, number + 300 FROM numbers(300);
-- the 600 rows of the merged part are now too few for a wide part
ALTER TABLE f_est MODIFY SETTING min_rows_for_wide_part = 1000;
ALTER TABLE f_real MODIFY SETTING min_rows_for_wide_part = 1000;
OPTIMIZE TABLE f_est FINAL;
OPTIMIZE TABLE f_real FINAL;

CREATE HYPOTHETICAL PROJECTION p ON f_est (SELECT b, sum(v) GROUP BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM f_est GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT 'parent part: ' || part_type FROM system.parts WHERE database = currentDatabase() AND table = 'f_est' AND active;
SELECT 'real projection part: ' || part_type || ', ' || toString(rows) || ' rows, ' || toString(marks - 1) || ' marks'
FROM system.projection_parts WHERE database = currentDatabase() AND table = 'f_real' AND name = 'p' AND active;
SELECT 'real: ' || toString(marks) || ' marks'
FROM (EXPLAIN ESTIMATE SELECT b, sum(v) FROM f_real GROUP BY b);

DROP HYPOTHETICAL PROJECTION p ON f_est;
DROP TABLE f_est;
DROP TABLE f_real;
