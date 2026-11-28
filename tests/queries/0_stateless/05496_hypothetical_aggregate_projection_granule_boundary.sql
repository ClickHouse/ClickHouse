-- with use_const_adaptive_granularity = 1 a merged wide part can have any granule size between the two layouts that the estimate builds
-- a granule size between them can put a granule boundary on the filtered key and select one more mark
-- the twin table has the projection materialized and shows its marks and the marks that the optimizer weighs for it

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

-- the implicit minmax indices of add_minmax_index_for_numeric_columns would prune the marks of the base table and change
-- the estimates and the real reads this test compares, so the tables opt out
CREATE TABLE k_est (b UInt64, v UInt8) ENGINE = MergeTree ORDER BY b
    SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 8192, index_granularity_bytes = 1024, use_const_adaptive_granularity = 1, min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0, merge_max_block_size = 8192, enable_vertical_merge_algorithm = 0, enable_block_number_column = 0,
        enable_block_offset_column = 0, ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE k_real AS k_est;
ALTER TABLE k_real ADD PROJECTION p (SELECT b, sum(v) GROUP BY b);
-- 300 groups in each part, 100 of them in both: 600 rows merge into 500 groups
INSERT INTO k_est SELECT number, 1 FROM numbers(300);
INSERT INTO k_est SELECT number + 200, 1 FROM numbers(300);
INSERT INTO k_real SELECT number, 1 FROM numbers(300);
INSERT INTO k_real SELECT number + 200, 1 FROM numbers(300);
OPTIMIZE TABLE k_est FINAL;
OPTIMIZE TABLE k_real FINAL;

CREATE HYPOTHETICAL PROJECTION p ON k_est (SELECT b, sum(v) GROUP BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM k_est WHERE b = 66 GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT 'real projection part: ' || toString(rows) || ' rows, ' || toString(marks - 1) || ' marks'
FROM system.projection_parts WHERE database = currentDatabase() AND table = 'k_real' AND name = 'p' AND active;
SELECT 'real: ' || trim(explain)
FROM (EXPLAIN projections = 1 SELECT b, sum(v) FROM k_real WHERE b = 66 GROUP BY b)
WHERE match(trim(explain), '^Description: ');

DROP HYPOTHETICAL PROJECTION p ON k_est;
DROP TABLE k_est;
DROP TABLE k_real;
