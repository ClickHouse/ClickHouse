-- `MATERIALIZE PROJECTION` writes the projection part of a level-zero part from merged states, so its granules can differ from those of an insert
-- one twin table has the projection from the start, the other one adds and materializes it after the insert

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

-- the implicit minmax indices of add_minmax_index_for_numeric_columns would prune the marks of the base table and change
-- the estimates and the real reads this test compares, so the tables opt out
CREATE TABLE m_est (a UInt16, b UInt16, v UInt8) ENGINE = MergeTree ORDER BY a
    SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 8192, index_granularity_bytes = 1024, use_const_adaptive_granularity = 0, min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0, enable_block_number_column = 0, enable_block_offset_column = 0, ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE m_inserted AS m_est;
CREATE TABLE m_materialized AS m_est;
ALTER TABLE m_inserted ADD PROJECTION p (SELECT b, uniqExact(v) GROUP BY b);
INSERT INTO m_est SELECT number, number % 500, 0 FROM numbers(2000);
INSERT INTO m_inserted SELECT number, number % 500, 0 FROM numbers(2000);
INSERT INTO m_materialized SELECT number, number % 500, 0 FROM numbers(2000);
ALTER TABLE m_materialized ADD PROJECTION p (SELECT b, uniqExact(v) GROUP BY b);
ALTER TABLE m_materialized MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE HYPOTHETICAL PROJECTION p ON m_est (SELECT b, uniqExact(v) GROUP BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, uniqExact(v) FROM m_est GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT table || ': parent level ' || toString(level) FROM system.parts
WHERE database = currentDatabase() AND table IN ('m_inserted', 'm_materialized') AND active ORDER BY table;
SELECT table || ': ' || toString(rows) || ' rows, ' || toString(marks - 1) || ' marks' FROM system.projection_parts
WHERE database = currentDatabase() AND table IN ('m_inserted', 'm_materialized') AND name = 'p' AND active ORDER BY table;
SELECT 'inserted: ' || toString(marks) || ' marks' FROM (EXPLAIN ESTIMATE SELECT b, uniqExact(v) FROM m_inserted GROUP BY b);
SELECT 'materialized: ' || toString(marks) || ' marks' FROM (EXPLAIN ESTIMATE SELECT b, uniqExact(v) FROM m_materialized GROUP BY b);

DROP HYPOTHETICAL PROJECTION p ON m_est;
DROP TABLE m_est;
DROP TABLE m_inserted;
DROP TABLE m_materialized;
