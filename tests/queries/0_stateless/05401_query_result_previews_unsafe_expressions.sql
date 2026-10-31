-- Query result previews (the `query_result_previews` setting) must neither change nor fail the
-- result of a query. A preview holds intermediate values: a stateful expression after the
-- aggregation keeps the preview emitters dormant, and a preview on which an expression throws is
-- dropped. With `max_threads = 1` and two source blocks, the aggregation below emits a preview with
-- `c = 32768` after the first block, and the final result has `c = 65536`.

SET query_result_previews = 1;
SET query_result_previews_min_interval_ms = 0;
SET max_threads = 1;
SET max_block_size = 65536;

SELECT '-- stateful function';
SELECT rowNumberInAllBlocks() AS rn, c FROM (SELECT countIf(number % 2 = 0) AS c FROM numbers(131072));

SELECT '-- stateful function in a lambda';
SELECT arrayMap(x -> x + rowNumberInAllBlocks(), [c]) FROM (SELECT countIf(number % 2 = 0) AS c FROM numbers(131072));

SELECT '-- function that throws on an intermediate value';
SELECT intDiv(1, c - 32768) FROM (SELECT countIf(number % 2 = 0) AS c FROM numbers(131072));

SELECT '-- HAVING that throws on an intermediate value';
SELECT number % 2 AS k, count() AS c FROM numbers(131072) GROUP BY k HAVING intDiv(1, c - 32768) = 0 ORDER BY k;

SELECT '-- the same queries without previews';
SET query_result_previews = 0;
SELECT rowNumberInAllBlocks() AS rn, c FROM (SELECT countIf(number % 2 = 0) AS c FROM numbers(131072));
SELECT arrayMap(x -> x + rowNumberInAllBlocks(), [c]) FROM (SELECT countIf(number % 2 = 0) AS c FROM numbers(131072));
SELECT intDiv(1, c - 32768) FROM (SELECT countIf(number % 2 = 0) AS c FROM numbers(131072));
SELECT number % 2 AS k, count() AS c FROM numbers(131072) GROUP BY k HAVING intDiv(1, c - 32768) = 0 ORDER BY k;
