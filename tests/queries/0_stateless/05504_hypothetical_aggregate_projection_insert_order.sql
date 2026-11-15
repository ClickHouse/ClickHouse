-- an insert calculates the projection from the rows in the order of the insert, the estimate reads them in the order of the part
-- `any` keeps the first value of a group, so the order of the rows changes the size of its state

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0, max_threads = 1;
SET max_block_size = 65409, group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;

CREATE TABLE o_est (id UInt64, k UInt16, s String) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 10, index_granularity_bytes = 10485760, use_const_adaptive_granularity = 1, min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, enable_block_number_column = 0,
        enable_block_offset_column = 0;
CREATE TABLE o_inserted AS o_est;
ALTER TABLE o_inserted ADD PROJECTION p (SELECT k, any(s) GROUP BY k) WITH SETTINGS (index_granularity = 8192, index_granularity_bytes = 1024);
-- the insert gives each group a value of 4096 bytes first and a value of 1 byte next, the part sorts the value of 1 byte first
INSERT INTO o_est SELECT if(number % 2 = 0, intDiv(number, 2) + 500, intDiv(number, 2)), intDiv(number, 2),
    if(number % 2 = 0, repeat('x', 4096), 'x') FROM numbers(1000);
INSERT INTO o_inserted SELECT if(number % 2 = 0, intDiv(number, 2) + 500, intDiv(number, 2)), intDiv(number, 2),
    if(number % 2 = 0, repeat('x', 4096), 'x') FROM numbers(1000);

CREATE HYPOTHETICAL PROJECTION p ON o_est (SELECT k, any(s) GROUP BY k)
    WITH SETTINGS (index_granularity = 8192, index_granularity_bytes = 1024);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT k, any(s) FROM o_est GROUP BY k)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
-- the part of the insert has one group in each granule, so the optimizer reads the base table
SELECT part_type || ', ' || toString(rows) || ' rows, ' || toString(marks - 1) || ' marks' FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'o_inserted' AND name = 'p' AND active;
SELECT toString(marks) || ' marks' FROM (EXPLAIN ESTIMATE SELECT k, any(s) FROM o_inserted GROUP BY k);

DROP HYPOTHETICAL PROJECTION p ON o_est;
DROP TABLE o_est;
DROP TABLE o_inserted;
