-- A preliminary `DISTINCT` must not coalesce its output above a view: the query of the view is stored
-- outside the plan tree and can stop emitting rows (here, an inner `DISTINCT` over an unbounded input),
-- so the coalesced keys would never reach the outer `LIMIT`.

SET allow_parallel_distinct = 1, max_threads = 4, max_block_size = 1000;
SET max_bytes_before_external_distinct = 0, max_bytes_ratio_before_external_distinct = 0;
SET optimize_distinct_in_order = 0, query_plan_remove_redundant_distinct = 0;
SET max_execution_time = 60, timeout_overflow_mode = 'throw';

DROP VIEW IF EXISTS coalescing_view;
CREATE VIEW coalescing_view SQL SECURITY DEFINER AS
SELECT DISTINCT number % 2 AS k FROM system.numbers_mt SETTINGS max_threads = 1;

SELECT count(), sum(k) FROM (SELECT * FROM (SELECT DISTINCT k FROM coalescing_view) LIMIT 2);

DROP VIEW coalescing_view;
