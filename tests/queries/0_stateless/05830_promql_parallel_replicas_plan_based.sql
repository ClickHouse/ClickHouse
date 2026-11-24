-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A PromQL query runs with plan-based parallel replicas reading from the remote replicas only:
-- the tags of the time series are read as data, a replica doesn't need any state of the initiator.
-- https://github.com/ClickHouse/ClickHouse/issues/131138

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS rx;

CREATE TABLE rx ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
INSERT INTO rx (metric_name, tags, samples) VALUES
    ('a_total', {'job': 'api'}, [(1000, 1.), (1060, 2.)]),
    ('a_total', {'job': 'web'}, [(1000, 6.), (1060, 8.)]),
    ('a_total', {'job': 'x'}, [(1000, 3.), (1060, 6.)]);

SET enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 0, max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT tags, value FROM prometheusQuery(rx, 'a_total{job!~"api|web"}', 1000) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(rx, 'sum by (job) (rate(a_total[2m]))', 1060) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(rx, 'label_replace(a_total, "j", "$1", "job", "(.*)")', 1060) ORDER BY tags;

DROP TABLE rx;
