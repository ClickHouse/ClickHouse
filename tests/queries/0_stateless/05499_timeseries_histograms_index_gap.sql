-- Prometheus addresses the buckets of a histogram with spans whose offset from the previous span is an Int32. The buckets are stored
-- as pairs of the bucket index and the count, so a direct INSERT could store a gap between two indexes which no span can express,
-- and remote read couldn't return it: the sink rejects it. The widest gap a span can express is accepted.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

-- From the index -2147483648 to 0 the span offset is 2147483647, the greatest Int32.
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'widest', map(), CAST([(toDateTime64(100, 3), 2, 1, [(-2147483648, 1), (0, 1)])],
    'Array(Tuple(timestamp DateTime64(3), count_int Int64, sum Float64, positive_int Array(Tuple(Int32, Int64))))');
-- One more makes the offset overflow. Nothing of the INSERT is written, the tags of the series included.
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'too_wide', map(), CAST([(toDateTime64(100, 3), 2, 1, [(-2147483648, 1), (1, 1)])],
    'Array(Tuple(timestamp DateTime64(3), count_int Int64, sum Float64, negative_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }

SELECT t.metric_name, s.histograms.positive_int
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS s JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON s.id = t.id
ORDER BY t.metric_name;
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts);

DROP TABLE ts;
