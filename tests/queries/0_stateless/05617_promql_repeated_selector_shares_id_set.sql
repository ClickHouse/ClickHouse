-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test counts the sets a query builds.

-- Selectors with the same matchers and time range share one set of series ids.
-- The counted queries pin the settings that build those sets during planning.

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples)
SELECT 'm', map('job', concat('j', toString(number % 3)), 'pod', concat('p', toString(number))),
       arrayMap(i -> (toDateTime64(1000000 + i * 15, 3), toFloat64(number * 100 + i)), range(20))
FROM numbers(10);

SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} + m{pod="p1"}', toDateTime64(1000285, 3))
SETTINGS log_comment = '05617_same_twice', use_index_for_in_with_subqueries = 1, use_query_condition_cache = 0;

SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} + m{pod="p1"} + m{pod="p1"}', toDateTime64(1000285, 3))
SETTINGS log_comment = '05617_same_three_times', use_index_for_in_with_subqueries = 1, use_query_condition_cache = 0;

SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} - m{pod="p1"} offset 1m', toDateTime64(1000285, 3))
SETTINGS log_comment = '05617_other_time_range', use_index_for_in_with_subqueries = 1, use_query_condition_cache = 0;

SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} + ignoring(pod) m{pod="p4"}', toDateTime64(1000285, 3))
SETTINGS log_comment = '05617_other_matchers', use_index_for_in_with_subqueries = 1, use_query_condition_cache = 0;

-- Without the primary-key analysis the sets are built in the pipeline of the query, where a set being built isn't waited for.
SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} + m{pod="p1"} + m{pod="p1"}', toDateTime64(1000285, 3))
SETTINGS use_index_for_in_with_subqueries = 0, max_threads = 1;

SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} + m{pod="p1"} + m{pod="p1"}', toDateTime64(1000285, 3))
SETTINGS use_index_for_in_with_subqueries = 0, max_threads = 2;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SetsBuiltFromSubquery']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05617_same_twice', '05617_same_three_times', '05617_other_time_range', '05617_other_matchers')
ORDER BY event_time_microseconds;

DROP TABLE ts;
