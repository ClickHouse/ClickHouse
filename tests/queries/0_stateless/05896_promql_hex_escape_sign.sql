-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

DROP TABLE IF EXISTS ts_str;

SET allow_experimental_time_series_table = 1;

CREATE TABLE ts_str ENGINE = TimeSeries;

SELECT hex(value) FROM prometheusQuery('ts_str', $$"\x+1"$$, 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT value FROM prometheusQuery('ts_str', $$"\u+041"$$, 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT value FROM prometheusQuery('ts_str', $$"\U+0000041"$$, 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT hex(value) FROM prometheusQuery('ts_str', $$"\u+D800"$$, 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }

SELECT value FROM prometheusQuery('ts_str', $$"\x41\x4aB\U00000043"$$, 1000);

DROP TABLE ts_str;
