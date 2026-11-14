-- Tags: no-fasttest, no-sanitizers, no-debug
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-sanitizers: the SQL of 40 nested aggregations needs more stack than sanitizer builds allow.
-- Tag no-debug: the same as no-sanitizers for the larger stack frames of debug builds.

-- PromQL info(): looking for the first selector in deeply nested aggregations must not take exponential time.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 1)]);

SELECT count() FROM prometheusQuery(ts, concat('info(', repeat('sum(', 40), 'vector(1)', repeat(')', 40), ')'), 0);

DROP TABLE ts;
