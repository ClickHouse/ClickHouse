SET session_timezone = 'UTC';
SET engine_file_truncate_on_insert = 1;

SELECT '--- leading whitespace of the help text survives a round trip';
INSERT INTO FUNCTION file(currentDatabase() || '_05876_help.om', OpenMetrics)
SELECT metric_name, help, 'gauge' AS type, [(toDateTime64(1, 3), 1.)] AS samples FROM
(
    SELECT 1 AS n, 'spaces' AS metric_name, '  two leading spaces' AS help
    UNION ALL SELECT 2, 'tab', '\tleading tab'
    UNION ALL SELECT 3, 'mixed', ' \t mixed, and trailing \t '
) ORDER BY n;
SELECT metric_name, help, length(help) FROM file(currentDatabase() || '_05876_help.om', OpenMetrics) ORDER BY metric_name FORMAT JSONEachRow;

SELECT '--- the separator before the help text is a single character, the rest is the text';
SELECT help, length(help) FROM format(OpenMetrics, concat('# HELP m   x', char(10), 'm 1', char(10), '# EOF', char(10))) FORMAT JSONEachRow;
SELECT help, length(help) FROM format(OpenMetrics, concat('# HELP m', char(10), 'm 1', char(10), '# EOF', char(10))) FORMAT JSONEachRow;
SELECT help, length(help) FROM format(OpenMetrics, concat('# HELP m ', char(10), 'm 1', char(10), '# EOF', char(10))) FORMAT JSONEachRow;

SELECT '--- a histogram point without the +Inf bucket is rejected';
SELECT 'h_bucket' AS metric_name, 'h' AS metric_family, 'histogram' AS type, map('le', '1') AS tags, [(toDateTime64(1, 3), 1.)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h_count' AS metric_name, 'h' AS metric_family, 'histogram' AS type, [(toDateTime64(1, 3), 1.)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'g_bucket' AS metric_name, 'g' AS metric_family, 'gaugehistogram' AS type, map('le', '1') AS tags, [(toDateTime64(1, 3), 1.)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
-- The +Inf bucket is required at every timestamp of the point, not only at some of them.
SELECT metric_name, 'h' AS metric_family, 'histogram' AS type, tags, samples FROM
(
    SELECT 1 AS n, 'h_bucket' AS metric_name, map('le', '1') AS tags, [(toDateTime64(1, 3), 1.), (toDateTime64(2, 3), 2.)] AS samples
    UNION ALL SELECT 2, 'h_bucket', map('le', '+Inf'), [(toDateTime64(1, 3), 1.)]
) ORDER BY n FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
-- And for every label set.
SELECT metric_name, 'h' AS metric_family, 'histogram' AS type, tags, [(toDateTime64(1, 3), 1.)] AS samples FROM
(
    SELECT 1 AS n, 'h_bucket' AS metric_name, map('le', '+Inf', 'job', 'a') AS tags
    UNION ALL SELECT 2, 'h_bucket', map('le', '1', 'job', 'b')
) ORDER BY n FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

SELECT '--- decreasing cumulative bucket counts are rejected';
SELECT metric_name, 'h' AS metric_family, 'histogram' AS type, tags, [(toDateTime64(1, 3), value)] AS samples FROM
(
    SELECT 1 AS n, 'h_bucket' AS metric_name, map('le', '1') AS tags, 5. AS value
    UNION ALL SELECT 2, 'h_bucket', map('le', '+Inf'), 3.
    UNION ALL SELECT 3, 'h_count', map(), 3.
    UNION ALL SELECT 4, 'h_sum', map(), 2.
) ORDER BY n FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
-- The order of the rows does not matter: the buckets are compared in the order of `le`.
SELECT metric_name, 'g' AS metric_family, 'gaugehistogram' AS type, tags, [(toDateTime64(1, 3), value)] AS samples FROM
(
    SELECT 1 AS n, 'g_bucket' AS metric_name, map('le', '+Inf') AS tags, 4. AS value
    UNION ALL SELECT 2, 'g_bucket', map('le', '10'), 2.
    UNION ALL SELECT 3, 'g_bucket', map('le', '1'), 3.
) ORDER BY n FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

SELECT '--- the count must equal the +Inf bucket';
SELECT metric_name, 'h' AS metric_family, 'histogram' AS type, tags, [(toDateTime64(1, 3), value)] AS samples FROM
(
    SELECT 1 AS n, 'h_bucket' AS metric_name, map('le', '+Inf') AS tags, 3. AS value
    UNION ALL SELECT 2, 'h_count', map(), 4.
) ORDER BY n FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

SELECT '--- valid points: several label sets and timestamps, rows in any order';
SELECT metric_name, 'h' AS metric_family, 'histogram' AS type, tags, samples FROM
(
    SELECT 1 AS n, 'h_count' AS metric_name, map('job', 'a') AS tags, [(toDateTime64(1, 3), 3.), (toDateTime64(2, 3), 5.)] AS samples
    UNION ALL SELECT 2, 'h_bucket', map('job', 'a', 'le', '+Inf'), [(toDateTime64(1, 3), 3.), (toDateTime64(2, 3), 5.)]
    UNION ALL SELECT 3, 'h_bucket', map('job', 'a', 'le', '1'), [(toDateTime64(1, 3), 3.), (toDateTime64(2, 3), 4.)]
    UNION ALL SELECT 4, 'h_sum', map('job', 'a'), [(toDateTime64(1, 3), 2.5), (toDateTime64(2, 3), 7.)]
    UNION ALL SELECT 5, 'h_bucket', map('job', 'b', 'le', '+Inf'), [(toDateTime64(1, 3), 0.)]
    UNION ALL SELECT 6, 'h_created', map('job', 'b'), [(toDateTime64(0, 3), 0.)]
) ORDER BY n FORMAT OpenMetrics;
