-- the parts that a merge reads can store a grouping key with default values as sparse, so their rows can be narrower than the merged groups
-- the twin table has the projection materialized and shows its marks and the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

-- the implicit minmax indices of add_minmax_index_for_numeric_columns would prune the marks of the base table and change
-- the estimates and the real reads this test compares, so the tables opt out
CREATE TABLE s_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY v
    SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 8192, index_granularity_bytes = 1024, use_const_adaptive_granularity = 1, min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0, merge_max_block_size = 8192, enable_vertical_merge_algorithm = 0, enable_block_number_column = 0,
        enable_block_offset_column = 0, ratio_of_defaults_for_sparse_serialization = 0.9375;
CREATE TABLE s_real AS s_est;
ALTER TABLE s_real ADD PROJECTION p (SELECT a, b, sum(v) GROUP BY a, b);
-- 300 groups in each part, 100 of them in both, and `a` is always 0
INSERT INTO s_est SELECT 0, number, number FROM numbers(300);
INSERT INTO s_est SELECT 0, number + 200, number + 300 FROM numbers(300);
INSERT INTO s_real SELECT 0, number, number FROM numbers(300);
INSERT INTO s_real SELECT 0, number + 200, number + 300 FROM numbers(300);
OPTIMIZE TABLE s_est FINAL;
OPTIMIZE TABLE s_real FINAL;

CREATE HYPOTHETICAL PROJECTION p ON s_est (SELECT a, b, sum(v) GROUP BY a, b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, sum(v) FROM s_est GROUP BY a, b)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT 'real projection part: ' || part_type || ', ' || toString(rows) || ' rows, ' || toString(marks - 1) || ' marks'
FROM system.projection_parts WHERE database = currentDatabase() AND table = 's_real' AND name = 'p' AND active;
SELECT 'real: ' || toString(marks) || ' marks'
FROM (EXPLAIN ESTIMATE SELECT a, b, sum(v) FROM s_real GROUP BY a, b);

DROP HYPOTHETICAL PROJECTION p ON s_est;
DROP TABLE s_est;
DROP TABLE s_real;
