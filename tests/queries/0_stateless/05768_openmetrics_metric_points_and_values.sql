-- `FORMAT OpenMetrics`: metric points of every type, typed sample values, and independent sibling families on input.

SET session_timezone = 'UTC';

SELECT '--- stateset: the states of one metric point are written together at each timestamp';
SELECT 'mode' AS metric_name, 'stateset' AS type, x.1 AS tags, x.2 AS samples
FROM (SELECT arrayJoin([
    (map('mode', 'a'), [(toDateTime64(1, 3), 1.), (toDateTime64(2, 3), 0.)]),
    (map('mode', 'b'), [(toDateTime64(1, 3), 0.), (toDateTime64(2, 3), 1.)])]) AS x)
FORMAT OpenMetrics;

SELECT '--- gauge: the fragments of one series are merged';
SELECT 'g' AS metric_name, 'gauge' AS type, map('l', x.1) AS tags, x.2 AS samples
FROM (SELECT arrayJoin([
    ('a', [(toDateTime64(1, 3), 1.)]),
    ('b', [(toDateTime64(1, 3), 2.)]),
    ('a', [(toDateTime64(2, 3), 3.)])]) AS x)
FORMAT OpenMetrics;

SELECT '--- counter: many fragments of one series are written in timestamp order';
SELECT 'c_total' AS metric_name, 'c' AS metric_family, 'counter' AS type,
    [(toDateTime64(t, 3), toFloat64(t))] AS samples
FROM (SELECT arrayJoin([1, 3, 5, 0, 2, 4]) AS t)
FORMAT OpenMetrics;

SELECT '--- typed values are validated';
SELECT 'mode' AS metric_name, 'stateset' AS type, map('mode', 'a') AS tags, [(toDateTime64(1, 3), 2.)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'mode' AS metric_name, 'stateset' AS type, map('other', 'a') AS tags, [(toDateTime64(1, 3), 1.)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'build_info' AS metric_name, 'build' AS metric_family, 'info' AS type, [(toDateTime64(1, 3), 42.)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'c_total' AS metric_name, 'c' AS metric_family, 'counter' AS type, [(toDateTime64(1, 3), -1.)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'c_total' AS metric_name, 'c' AS metric_family, 'counter' AS type, [(toDateTime64(1, 3), nan)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h_bucket' AS metric_name, 'h' AS metric_family, 'histogram' AS type, map('le', '+Inf') AS tags, [(toDateTime64(1, 3), 1.5)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 's' AS metric_name, 'summary' AS type, map('quantile', '0.5') AS tags, [(toDateTime64(1, 3), -1.)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

SELECT '--- values that the types allow';
SELECT 'g_gsum' AS metric_name, 'g' AS metric_family, 'gaugehistogram' AS type, [(toDateTime64(1, 3), -1.5)] AS samples FORMAT OpenMetrics;
SELECT 't' AS metric_name, 'gauge' AS type, [(toDateTime64(1, 3), -1.5), (toDateTime64(2, 3), nan)] AS samples FORMAT OpenMetrics;

SELECT '--- input: independent families named like siblings';
SELECT metric_name, metric_family, type, samples
FROM format(OpenMetrics, '# TYPE x_sum gauge\nx_sum 1 1\n# TYPE x gauge\nx 2 1\n# EOF\n')
ORDER BY metric_name;
SELECT metric_name FROM format(OpenMetrics, 'x_sum 1 1\n# TYPE x summary\nx_count 2 1\n# EOF\n'); -- { serverError INCORRECT_DATA }
SELECT metric_name FROM format(OpenMetrics, '# HELP x help\nx_sum 1 1\n# TYPE x summary\nx_count 2 1\n# EOF\n'); -- { serverError INCORRECT_DATA }
