-- `FORMAT OpenMetrics`: the samples of one metric point stay together, unsuffixed metric types
-- require the sample name to be the family name, families without samples keep their descriptors,
-- and `# HELP` text is escaped on output and unescaped on input.

SET session_timezone = 'UTC';

SELECT '--- histogram with two timestamps: each point is contiguous';
SELECT * FROM
(
    SELECT 'h_bucket' AS metric_name, 'h' AS metric_family, 'histogram' AS type,
           [('le', '1')] AS tags, [(toDateTime64(1, 3), 1.0), (toDateTime64(2, 3), 2.0)] AS samples
    UNION ALL
    SELECT 'h_bucket', 'h', 'histogram', [('le', '+Inf')], [(toDateTime64(1, 3), 3.0), (toDateTime64(2, 3), 4.0)]
    UNION ALL
    SELECT 'h_count', 'h', 'histogram', [], [(toDateTime64(1, 3), 3.0), (toDateTime64(2, 3), 4.0)]
    UNION ALL
    SELECT 'h_sum', 'h', 'histogram', [], [(toDateTime64(1, 3), 5.0), (toDateTime64(2, 3), 6.0)]
)
ORDER BY metric_family, metric_name, tags
FORMAT OpenMetrics;

SELECT '--- histogram round trip through the reader';
SELECT metric_name, metric_family, type, tags, samples FROM format(OpenMetrics, $$
# TYPE h histogram
h_bucket{le="1"} 1 1
h_bucket{le="+Inf"} 3 1
h_count 3 1
h_sum 5 1
h_bucket{le="1"} 2 2
h_bucket{le="+Inf"} 4 2
h_count 4 2
h_sum 6 2
# EOF
$$) ORDER BY metric_name, tags;

SELECT '--- counter with two timestamps and two label sets';
SELECT * FROM
(
    SELECT 'c_total' AS metric_name, 'c' AS metric_family, 'counter' AS type,
           [('a', '1')] AS tags, [(toDateTime64(1, 3), 1.0), (toDateTime64(2, 3), 2.0)] AS samples
    UNION ALL
    SELECT 'c_created', 'c', 'counter', [('a', '1')], [(toDateTime64(1, 3), 0.0), (toDateTime64(2, 3), 0.0)]
    UNION ALL
    SELECT 'c_total', 'c', 'counter', [('a', '2')], [(toDateTime64(1, 3), 7.0)]
)
ORDER BY metric_family, tags, metric_name DESC
FORMAT OpenMetrics;

SELECT '--- unsuffixed types require the family name';
SELECT 'foo' AS metric_name, 'bar' AS metric_family, 'gauge' AS type, [(toDateTime64(1, 3), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'foo' AS metric_name, 'bar' AS metric_family, 'unknown' AS type, [(toDateTime64(1, 3), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'foo' AS metric_name, 'bar' AS metric_family, 'stateset' AS type, [(toDateTime64(1, 3), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'foo' AS metric_name, 'bar' AS metric_family, '' AS type, [(toDateTime64(1, 3), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'bar' AS metric_name, 'bar' AS metric_family, 'gauge' AS type, [(toDateTime64(1, 3), 1.0)] AS samples FORMAT OpenMetrics;

SELECT '--- metadata-only family on output';
SELECT '' AS metric_name, 'unused' AS metric_family, 'gauge' AS type, 'Not used' AS help,
       CAST([], 'Array(Tuple(String, String))') AS tags, CAST([], 'Array(Tuple(DateTime64(3), Float64))') AS samples
FORMAT OpenMetrics;

SELECT '--- metadata-only family on input';
SELECT metric_name, metric_family, help, type, unit, tags, samples FROM format(OpenMetrics, $$
# HELP unused Not used
# TYPE unused gauge
# TYPE used gauge
used 1 1
# EOF
$$) ORDER BY metric_family;

SELECT '--- HELP escaping on output';
SELECT 'm' AS metric_name, concat('a\\nb', char(10), 'c "d"') AS help, [(toDateTime64(1, 3), 1.0)] AS samples FORMAT OpenMetrics;

SELECT '--- HELP unescaping on input';
SELECT help, hex(help) FROM format(OpenMetrics, 'metric_name String, help String', $$
# HELP m a\\nb\nc \"d\"
m 1 1
# EOF
$$);

SELECT help FROM format(OpenMetrics, 'metric_name String, help String', $$
# HELP m bad \x escape
m 1 1
# EOF
$$); -- { serverError INCORRECT_DATA }

SELECT '--- TimeSeries round trip with a two-timestamp histogram, escaped help, and a family without samples';
SET allow_experimental_time_series_table = 1;
DROP TABLE IF EXISTS ts_om_review;
CREATE TABLE ts_om_review ENGINE = TimeSeries;
INSERT INTO ts_om_review (metric_name, metric_family, help, type, unit, tags, samples)
SELECT metric_name, metric_family, help, type, unit, CAST(tags, 'Map(String, String)'), samples FROM format(OpenMetrics, $$
# HELP h Latency with a \\ backslash\nand a second line
# TYPE h histogram
h_bucket{le="1"} 1 1
h_bucket{le="+Inf"} 3 1
h_count 3 1
h_sum 5 1
h_bucket{le="1"} 2 2
h_bucket{le="+Inf"} 4 2
h_count 4 2
h_sum 6 2
# HELP unused Not used
# TYPE unused gauge
# EOF
$$);
SELECT * FROM ts_om_review ORDER BY metric_family, metric_name, tags FORMAT OpenMetrics;
DROP TABLE ts_om_review;
