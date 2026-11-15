-- the settings of an insert are not in its part, and they can make the insert copy the states into memory of their own
-- the insert of f_inserted gets smaller blocks of the aggregation than the estimate, so it writes a wide part

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;
SET max_block_size = 65409, group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;

CREATE TABLE f_est (b UInt64, v UInt8) ENGINE = MergeTree ORDER BY b
    SETTINGS index_granularity = 48, index_granularity_bytes = 1024, use_const_adaptive_granularity = 1, min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0;
CREATE TABLE f_inserted AS f_est;
CREATE TABLE f_materialized AS f_est;
-- 500 groups take 8000 bytes in one calculated block and 12000 bytes as states in memory of their own
ALTER TABLE f_inserted ADD PROJECTION p (SELECT b, sum(v) GROUP BY b)
    WITH SETTINGS (index_granularity = 8192, min_bytes_for_wide_part = 10000);
INSERT INTO f_est SELECT number, 1 FROM numbers(500);
INSERT INTO f_inserted SETTINGS max_block_size = 100 SELECT number, 1 FROM numbers(500);
INSERT INTO f_materialized SELECT number, 1 FROM numbers(500);
ALTER TABLE f_materialized ADD PROJECTION p (SELECT b, sum(v) GROUP BY b)
    WITH SETTINGS (index_granularity = 8192, min_bytes_for_wide_part = 10000);
ALTER TABLE f_materialized MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

-- an insert writes a wide part of 12 marks or a compact part of 11 marks, the part does not tell which
CREATE HYPOTHETICAL PROJECTION p ON f_est (SELECT b, sum(v) GROUP BY b)
    WITH SETTINGS (index_granularity = 8192, min_bytes_for_wide_part = 10000);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM f_est GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT table || ': ' || part_type || ', ' || toString(rows) || ' rows, ' || toString(marks - 1) || ' marks' FROM system.projection_parts
WHERE database = currentDatabase() AND table IN ('f_inserted', 'f_materialized') AND name = 'p' AND active ORDER BY table;
SELECT 'inserted: ' || toString(marks) || ' marks' FROM (EXPLAIN ESTIMATE SELECT b, sum(v) FROM f_inserted GROUP BY b);
SELECT 'materialized: ' || toString(marks) || ' marks' FROM (EXPLAIN ESTIMATE SELECT b, sum(v) FROM f_materialized GROUP BY b);

DROP HYPOTHETICAL PROJECTION p ON f_est;
DROP TABLE f_est;
DROP TABLE f_inserted;
DROP TABLE f_materialized;
