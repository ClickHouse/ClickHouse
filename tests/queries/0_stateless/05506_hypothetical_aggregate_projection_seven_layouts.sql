-- a level-zero part over the `min_insert_block_size_rows` of the background profile has seven possible projection layouts:
-- three in each part format and the layout of `MATERIALIZE PROJECTION`

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;
SET max_block_size = 65409, group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;

CREATE TABLE s (a UInt32, b UInt16, v UInt8) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, use_const_adaptive_granularity = 0, min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0, ratio_of_defaults_for_sparse_serialization = 1,
        merge_max_block_size = 8192;
INSERT INTO s SELECT number, number % 500, 0 FROM numbers(1100000)
    SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 2000000, min_insert_block_size_bytes = 0;
SELECT 'parts: ' || toString(count()) || ', level ' || toString(max(level)) FROM system.parts
WHERE database = currentDatabase() AND table = 's' AND active;

CREATE HYPOTHETICAL PROJECTION p ON s (SELECT b, uniqExact(v) GROUP BY b) WITH SETTINGS (min_bytes_for_wide_part = 10000);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, uniqExact(v) FROM s GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');

DROP HYPOTHETICAL PROJECTION p ON s;
DROP TABLE s;
