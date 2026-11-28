-- a projection part can be wide when its parent part is compact, and a merge keeps that format after a change of min_rows_for_wide_part
-- the twin table has the projection materialized and shows its format, its marks and the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

-- a parent row has fewer bytes than a group of the projection, so only the projection reaches min_bytes_for_wide_part
CREATE TABLE c_est (b UInt64, v UInt8, a FixedString(6)) ENGINE = MergeTree ORDER BY v
    SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, use_const_adaptive_granularity = 1, min_bytes_for_wide_part = 4600,
        min_rows_for_wide_part = 0, merge_max_block_size = 8192, enable_vertical_merge_algorithm = 0, enable_block_number_column = 0,
        enable_block_offset_column = 0, ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE c_real AS c_est;
ALTER TABLE c_real ADD PROJECTION p (SELECT b, sum(v) GROUP BY b);
-- 300 groups in each part, 100 of them in both: 600 rows merge into 500 groups
INSERT INTO c_est SELECT number, number, '' FROM numbers(300);
INSERT INTO c_est SELECT number + 200, number, '' FROM numbers(300);
INSERT INTO c_real SELECT number, number, '' FROM numbers(300);
INSERT INTO c_real SELECT number + 200, number, '' FROM numbers(300);
SELECT 'parts before the merge: ' || arrayStringConcat(groupArray(part_type), ', ') FROM system.parts
WHERE database = currentDatabase() AND table = 'c_real' AND active;
SELECT 'projection parts before the merge: ' || arrayStringConcat(groupArray(part_type), ', ') FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'c_real' AND name = 'p' AND active;
-- the 600 rows of the merged part are now too few for a wide part
ALTER TABLE c_est MODIFY SETTING min_rows_for_wide_part = 1000;
ALTER TABLE c_real MODIFY SETTING min_rows_for_wide_part = 1000;
OPTIMIZE TABLE c_est FINAL;
OPTIMIZE TABLE c_real FINAL;

CREATE HYPOTHETICAL PROJECTION p ON c_est (SELECT b, sum(v) GROUP BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM c_est GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT 'parent part: ' || part_type FROM system.parts WHERE database = currentDatabase() AND table = 'c_est' AND active;
SELECT 'real projection part: ' || part_type || ', ' || toString(rows) || ' rows, ' || toString(marks - 1) || ' marks'
FROM system.projection_parts WHERE database = currentDatabase() AND table = 'c_real' AND name = 'p' AND active;
SELECT 'real: ' || toString(marks) || ' marks'
FROM (EXPLAIN ESTIMATE SELECT b, sum(v) FROM c_real GROUP BY b);

DROP HYPOTHETICAL PROJECTION p ON c_est;
DROP TABLE c_est;
DROP TABLE c_real;
