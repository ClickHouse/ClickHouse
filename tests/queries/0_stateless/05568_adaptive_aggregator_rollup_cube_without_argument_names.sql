-- The `ROLLUP` and `CUBE` steps merge aggregate states, and their aggregates carry no argument names, so their
-- aggregators never run the adaptive aggregation, whatever the settings. Each query uses the aggregate functions that
-- `adaptive_aggregator_combine_arg_min_max` applies to, with both analyzers.

SET enable_adaptive_aggregator = 1, adaptive_aggregator_combine_arg_min_max = 1;

SELECT 'ROLLUP';
SELECT k, max(number), argMax(number * 2, number), count()
FROM (SELECT number, intDiv(number, 10) % 3 AS k FROM numbers(100))
GROUP BY k WITH ROLLUP ORDER BY k, count() SETTINGS enable_analyzer = 1;
SELECT k, max(number), argMax(number * 2, number), count()
FROM (SELECT number, intDiv(number, 10) % 3 AS k FROM numbers(100))
GROUP BY k WITH ROLLUP ORDER BY k, count() SETTINGS enable_analyzer = 0;

SELECT 'CUBE';
SELECT k, min(number), argMin(number * 2, number), count()
FROM (SELECT number, intDiv(number, 10) % 3 AS k FROM numbers(100))
GROUP BY k WITH CUBE ORDER BY k, count() SETTINGS enable_analyzer = 1;
SELECT k, min(number), argMin(number * 2, number), count()
FROM (SELECT number, intDiv(number, 10) % 3 AS k FROM numbers(100))
GROUP BY k WITH CUBE ORDER BY k, count() SETTINGS enable_analyzer = 0;
