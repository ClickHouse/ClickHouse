-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS prometheus;
CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('foo', map('job', 'a'), [(toDateTime64(100, 3), 10)]),
    ('foo', map('job', 'b'), [(toDateTime64(100, 3), 20)]),
    ('multi', map('job', 'a\nb'), [(toDateTime64(100, 3), 5)]);

SELECT '-- series with and without a dropped metric name aggregate into one bucket, which drops the name';
SELECT tags, value FROM prometheusQuery('prometheus', 'sum by (__name__) ((foo{job="a"} + 1) or foo{job="b"})', 100) ORDER BY tags;
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile by (__name__) (1, (foo{job="a"} + 1) or foo{job="b"})', 100) ORDER BY tags;
SELECT tags, value FROM prometheusQuery('prometheus', 'topk(1, (foo{job="a"} + 1) or foo{job="b"}) by (__name__)', 100) ORDER BY tags;
SELECT count() FROM prometheusQuery('prometheus', 'limitk(1, (foo{job="a"} + 1) or foo{job="b"}) by (__name__)', 100);

SELECT '-- a bucket without a dropped metric name keeps the name';
SELECT tags, value FROM prometheusQuery('prometheus', 'sum by (__name__) (foo)', 100) ORDER BY tags;

SELECT '-- the regex of label_replace matches a newline in the source label';
SELECT tags, value FROM prometheusQuery('prometheus', 'label_replace(multi + 1, "__name__", "renamed", "job", ".*")', 100);

SELECT '-- a metric name with a newline is dropped';
SELECT tags, value FROM prometheusQuery('prometheus', 'label_replace(vector(1), "__name__", "foo\\nbar", "", "") + 1', 100);

DROP TABLE prometheus;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('foo', map('job', 'a'), [(toDateTime64(100, 3), 10)]),
    ('h_bucket', map('le', '1'), [(toDateTime64(100, 3), 1)]),
    ('h_bucket', map('le', '+Inf'), [(toDateTime64(100, 3), 2)]);

SELECT '-- buckets of one histogram stay in one group when only some of them dropped the metric name';
SELECT tags, value FROM prometheusQuery('prometheus', 'histogram_quantile(0.5, (h_bucket{le="1"} + 0) or h_bucket{le="+Inf"})', 100);

SELECT '-- an or operand with an all-NULL grid does not drop the metric name';
SELECT tags, value FROM prometheusQuery('prometheus', 'sum by (__name__) (foo or (foo + 1))', 100) ORDER BY tags;
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile by (__name__) (1, foo or (foo + 1))', 100) ORDER BY tags;

DROP TABLE prometheus;
