-- each part that a merge reads stores its own dictionary of a `LowCardinality` grouping key, so its rows can be wider than the merged groups
-- the twin table has the projection materialized and shows its marks and the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

-- the merge byte limits stop background merges until OPTIMIZE
CREATE TABLE l_est (b UInt64, v UInt8, pad FixedString(15)) ENGINE = MergeTree ORDER BY b
    SETTINGS index_granularity = 8192, index_granularity_bytes = 128, min_index_granularity_bytes = 0, use_const_adaptive_granularity = 1,
        min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, merge_max_block_size = 8192, enable_vertical_merge_algorithm = 0,
        enable_block_number_column = 0, enable_block_offset_column = 0, ratio_of_defaults_for_sparse_serialization = 1,
        max_bytes_to_merge_at_max_space_in_pool = 1, max_bytes_to_merge_at_min_space_in_pool = 1;
CREATE TABLE l_real AS l_est;
ALTER TABLE l_real ADD PROJECTION p (SELECT toLowCardinality(leftPad(toString(b), 5, '0')) AS k, sum(v) GROUP BY k);
-- 16 parts of one row each
INSERT INTO l_est SELECT number, 1, '' FROM numbers(16)
    SETTINGS max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
INSERT INTO l_real SELECT number, 1, '' FROM numbers(16)
    SETTINGS max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SELECT 'parts: ' || toString(count()) FROM system.parts WHERE database = currentDatabase() AND table = 'l_real' AND active;
OPTIMIZE TABLE l_est FINAL;
OPTIMIZE TABLE l_real FINAL;

CREATE HYPOTHETICAL PROJECTION p ON l_est (SELECT toLowCardinality(leftPad(toString(b), 5, '0')) AS k, sum(v) GROUP BY k);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT toLowCardinality(leftPad(toString(b), 5, '0')) AS k, sum(v) FROM l_est GROUP BY k)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT 'real projection part: ' || part_type || ', ' || toString(rows) || ' rows, ' || toString(marks - 1) || ' marks'
FROM system.projection_parts WHERE database = currentDatabase() AND table = 'l_real' AND name = 'p' AND active;
SELECT 'real: ' || toString(marks) || ' marks'
FROM (EXPLAIN ESTIMATE SELECT toLowCardinality(leftPad(toString(b), 5, '0')) AS k, sum(v) FROM l_real GROUP BY k);

DROP HYPOTHETICAL PROJECTION p ON l_est;
DROP TABLE l_est;
DROP TABLE l_real;
