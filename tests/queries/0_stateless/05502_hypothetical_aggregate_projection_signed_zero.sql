-- an insert groups the rows of a projection by a hash table, which keeps 0 and -0 apart, and `MATERIALIZE PROJECTION` merges them as equal
-- one twin table has the projection from the start, the other one adds and materializes it after the insert

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

CREATE TABLE z_est (id UInt16, k Float64, v UInt8) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 200, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        enable_block_number_column = 0, enable_block_offset_column = 0;
CREATE TABLE z_inserted AS z_est;
CREATE TABLE z_materialized AS z_est;
ALTER TABLE z_inserted ADD PROJECTION p (SELECT id, k, sum(v) GROUP BY id, k) WITH SETTINGS (index_granularity = 100);
-- each id has a row with 0 and a row with -0
INSERT INTO z_est SELECT intDiv(number, 2), if(number % 2, -toFloat64(0), toFloat64(0)), 1 FROM numbers(2000);
INSERT INTO z_inserted SELECT intDiv(number, 2), if(number % 2, -toFloat64(0), toFloat64(0)), 1 FROM numbers(2000);
INSERT INTO z_materialized SELECT intDiv(number, 2), if(number % 2, -toFloat64(0), toFloat64(0)), 1 FROM numbers(2000);
ALTER TABLE z_materialized ADD PROJECTION p (SELECT id, k, sum(v) GROUP BY id, k) WITH SETTINGS (index_granularity = 100);
ALTER TABLE z_materialized MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE HYPOTHETICAL PROJECTION p ON z_est (SELECT id, k, sum(v) GROUP BY id, k) WITH SETTINGS (index_granularity = 100);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT id, k, sum(v) FROM z_est GROUP BY id, k)
WHERE match(line, '^(Baseline|With |marks:|rows:|marks_span:|verdict:)');
SELECT table || ': ' || toString(rows) || ' rows, ' || toString(marks - 1) || ' marks' FROM system.projection_parts
WHERE database = currentDatabase() AND table IN ('z_inserted', 'z_materialized') AND name = 'p' AND active ORDER BY table;
SELECT 'inserted: ' || toString(marks) || ' marks' FROM (EXPLAIN ESTIMATE SELECT id, k, sum(v) FROM z_inserted GROUP BY id, k);
SELECT 'materialized: ' || toString(marks) || ' marks' FROM (EXPLAIN ESTIMATE SELECT id, k, sum(v) FROM z_materialized GROUP BY id, k);

DROP HYPOTHETICAL PROJECTION p ON z_est;
DROP TABLE z_est;
DROP TABLE z_inserted;
DROP TABLE z_materialized;
