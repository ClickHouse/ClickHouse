-- Tags: no-fasttest, no-flaky-check
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-flaky-check: the PromQL lexer is too slow under the thread fuzzer for a 100 KB query text.

-- A regex with very deeply nested groups does not overflow the stack.

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS rx_deep;
CREATE TABLE rx_deep ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO rx_deep (metric_name, tags, samples) VALUES ('a_total', {'job':'api'}, [(1000, 1.)]);

-- No series has this metric name, so the very deep regex is parsed but never run on rows.
SELECT count() FROM timeSeriesSelector(rx_deep, concat('{__name__="no_such_metric", job=~"', repeat('(', 50000), 'api', repeat(')', 50000), '"}'), 0, 2000, 'timestamp_and_value');

DROP TABLE rx_deep;
