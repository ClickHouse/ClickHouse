-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Before `timeSeriesSelector` reads the samples table, it probes the tags table for a series that
-- contradicts a whole-metric primary-key range (see 04836). The probe is a planning step of the
-- initiator: it runs with parallel replicas disabled, so it never sends a query to another replica.
-- The samples read that follows keeps the session settings and still runs on the parallel replicas
-- cluster. The two probe outcomes are covered: an exhausted probe (`foo`: every series of the metric
-- passes, the probe scans the tags table to the end and finds no row) and an early counterexample
-- (`foo{env="prod"}`: the `env="dev"` series fails the matcher, the probe stops at its first row).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_probe_local;

CREATE TABLE ts_probe_local ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));

INSERT INTO ts_probe_local (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]),
    ('bar', map('env', 'dev'), [(toDateTime64(100, 3), 100.)]);

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
-- Without a local plan the initiator consumes the replica streams to the end, so every replica query
-- is in `system.query_log` when the selector returns.
SET parallel_replicas_local_plan = 0;

SELECT '-- exhausted probe: the selector matches the whole metric';
SELECT timestamp, value FROM timeSeriesSelector(ts_probe_local, 'foo', 0, 1000) ORDER BY value, timestamp
SETTINGS log_comment = '05446_exhausted';

SELECT '-- early counterexample: a series of the metric fails the matcher';
SELECT timestamp, value FROM timeSeriesSelector(ts_probe_local, 'foo{env="prod"}', 0, 1000) ORDER BY value, timestamp
SETTINGS log_comment = '05446_early';

SYSTEM FLUSH LOGS query_log;

-- A replica query is scoped by the database it reads, not by `current_database`. It reads the inner
-- tables of this test. The samples read carries the tags subquery inside `id IN (...)`, so a replica
-- query is attributed to the probe when it does not read the samples table at all.
SELECT '-- what the replicas were sent: the samples read only, never the probe';
SELECT
    log_comment,
    countIf(query NOT LIKE '%.inner_id.samples.%') = 0 AS probe_stayed_local,
    countIf(query LIKE '%.inner_id.samples.%') > 0 AS samples_used_parallel_replicas
FROM system.query_log
WHERE has(databases, currentDatabase()) AND NOT is_initial_query
  AND log_comment IN ('05446_exhausted', '05446_early')
GROUP BY log_comment ORDER BY log_comment
SETTINGS enable_parallel_replicas = 0;

DROP TABLE ts_probe_local;
