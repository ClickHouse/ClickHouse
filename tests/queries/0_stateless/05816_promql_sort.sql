-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- PromQL functions `sort` and `sort_desc` order an instant vector by sample value, with NaN last in both
-- directions, as in Prometheus. Series with equal values are ordered by their labels, so the result is
-- deterministic. The order is applied only when `sort*` is the root of an instant query: under another
-- function, inside a subquery, or in a range query, the result keeps the usual order by labels.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- The data of `functions.test` for sort and sort_desc (Prometheus): One sample every 300 seconds,
-- and 11 samples 0, 10, ..., 100 at 0, 300, ..., 3000 seconds.
-- `step = 0` stands for the series of 11 NaN samples. The native histogram series is left out;
-- the `TimeSeries` engine has no native histograms yet.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'http_requests', tags, arrayMap(i -> (toDateTime64(i * 300, 3), if(step = 0, nan, toFloat64(i * step))), range(11))
FROM VALUES('tags Map(String, String), step UInt32',
    (map('job', 'api-server', 'instance', '0', 'group', 'production'), 10),
    (map('job', 'api-server', 'instance', '1', 'group', 'production'), 20),
    (map('job', 'api-server', 'instance', '0', 'group', 'canary'), 30),
    (map('job', 'api-server', 'instance', '1', 'group', 'canary'), 40),
    (map('job', 'api-server', 'instance', '2', 'group', 'canary'), 0),
    (map('job', 'app-server', 'instance', '0', 'group', 'production'), 50),
    (map('job', 'app-server', 'instance', '1', 'group', 'production'), 60),
    (map('job', 'app-server', 'instance', '0', 'group', 'canary'), 70),
    (map('job', 'app-server', 'instance', '1', 'group', 'canary'), 80));

-- Three series with the same value, inserted out of label order, to check how ties are ordered.
-- Prometheus returns ties in the order the series were created, which a table cannot reproduce,
-- so here ties are ordered by labels: ascending for `sort`, descending for `sort_desc`.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('ties', map('instance', 'c'), [(toDateTime64(3000, 3), 5)]),
    ('ties', map('instance', 'a'), [(toDateTime64(3000, 3), 5)]),
    ('ties', map('instance', 'b'), [(toDateTime64(3000, 3), 5)]);

SELECT '-- sort';
SELECT * FROM prometheusQuery('ts', 'sort(http_requests)', 3000);

SELECT '-- sort_desc';
SELECT * FROM prometheusQuery('ts', 'sort_desc(http_requests)', 3000);

SELECT '-- sort ties';
SELECT * FROM prometheusQuery('ts', 'sort(ties)', 3000);

SELECT '-- sort_desc ties';
SELECT * FROM prometheusQuery('ts', 'sort_desc(ties)', 3000);

SELECT '-- scalar vector';
SELECT * FROM prometheusQuery('ts', 'sort(vector(1))', 3000);

-- Not the root: `abs` is. Prometheus keeps the order of `sort_desc` here; this implementation orders by labels.
SELECT '-- under abs';
SELECT * FROM prometheusQuery('ts', 'abs(sort_desc(http_requests))', 3000);

SELECT '-- in subquery';
SELECT * FROM prometheusQuery('ts', 'last_over_time(sort_desc(http_requests)[5m:1m])', 3000);

-- In a range query `sort*` has no effect and the rows stay ordered by labels, as in Prometheus. Prometheus also
-- returns the warning "sort is ineffective for range queries since results are always ordered by labels"
-- for `sort`, `sort_desc` and `sort_by_label*`; this implementation returns no warning.
SELECT '-- range query';
SELECT * FROM prometheusQueryRange('ts', 'sort_desc(http_requests)', 2400, 3000, 300);

SELECT '-- wrong arguments';
SELECT * FROM prometheusQuery('ts', 'sort(http_requests, http_requests)', 3000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'sort(1)', 3000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE ts;
