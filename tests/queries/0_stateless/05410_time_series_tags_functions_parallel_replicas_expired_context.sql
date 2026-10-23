-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- With parallel replicas and an empty samples table, the local plan is built from a copy of the query tree and
-- runs `timeSeriesStoreTags` after the context it was created with is gone.

SET allow_experimental_time_series_table = 1, session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS t_data;
DROP TABLE IF EXISTS t_tags;
DROP TABLE IF EXISTS t_ranges;

CREATE TABLE ts ENGINE = TimeSeries;
CREATE TABLE t_data (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_tags (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_ranges (id UInt64) ENGINE = MergeTree ORDER BY id;

-- Time series with tags and time ranges but without samples.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('req', map('job', 'api', 'instance', 'i1'), []),
    ('req', map('job', 'db', 'instance', 'i2'), []),
    ('lim', map('job', 'api', 'instance', 'i1'), []);
-- Without parallel replicas: a remote replica would look up the bare `ts` of `timeSeriesTags(ts)` in its default database.
INSERT INTO FUNCTION timeSeriesTimeRanges(ts) SELECT id, toDateTime64(90, 3), toDateTime64(110, 3) FROM timeSeriesTags(ts)
SETTINGS enable_parallel_replicas = 0;
INSERT INTO t_tags VALUES (1), (2), (3);
INSERT INTO t_ranges VALUES (1), (2), (3);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1, automatic_parallel_replicas_mode = 0,
    parallel_replicas_prefer_local_replica = 1, max_execution_time_leaf = 0;

SELECT 'empty data';
SELECT * FROM prometheusQueryRange('ts', 'req', 100, 120, 10) SETTINGS log_comment = '05410_empty_data';
SELECT * FROM prometheusQueryRange('ts', 'lim / on(instance) req{instance="i1"}', 100, 120, 10) SETTINGS log_comment = '05410_empty_data';
SELECT * FROM prometheusQueryRange('ts', 'count_values("v", req)', 100, 120, 10) SETTINGS log_comment = '05410_empty_data';
SELECT id FROM (SELECT id FROM (SELECT id FROM t_data WHERE id IN (SELECT timeSeriesStoreTags(t.id, [('a', 'b')]) FROM t_tags AS t ALL LEFT JOIN (SELECT id, 1 AS matched FROM t_ranges) AS r USING (id) WHERE r.matched = 1)))
SETTINGS log_comment = '05410_empty_data';
SELECT id FROM (SELECT id FROM (SELECT id FROM t_data WHERE id IN (SELECT timeSeriesTagsToGroup([('a', toString(t.id))]) FROM t_tags AS t ALL LEFT JOIN (SELECT id, 1 AS matched FROM t_ranges) AS r USING (id) WHERE r.matched = 1)))
SETTINGS log_comment = '05410_empty_data';

SYSTEM FLUSH LOGS query_log;
SELECT 'empty data used parallel replicas', countIf(ProfileEvents['ParallelReplicasQueryCount'] > 0), count()
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment = '05410_empty_data'
SETTINGS enable_parallel_replicas = 0;

INSERT INTO FUNCTION timeSeriesSamples(ts) SELECT id, toDateTime64(100, 3), 5 FROM timeSeriesTags(ts)
SETTINGS enable_parallel_replicas = 0;

SELECT 'with data';
SELECT tags, samples FROM prometheusQueryRange('ts', 'req', 100, 120, 10) ORDER BY toString(tags);
SELECT tags, samples FROM prometheusQueryRange('ts', 'lim / on(instance) req{instance="i1"}', 100, 120, 10) ORDER BY toString(tags);

SELECT 'without parallel replicas';
SELECT number AS id, timeSeriesIdToTags(timeSeriesStoreTags(id, [('a', 'b')])) FROM numbers(1, 2) ORDER BY id SETTINGS enable_parallel_replicas = 0;
SELECT timeSeriesGroupToTags(timeSeriesTagsToGroup([('a', toString(number))])) FROM numbers(2) ORDER BY number SETTINGS enable_parallel_replicas = 0;
SELECT name, signature != '' FROM system.functions WHERE name IN ('timeSeriesStoreTags', 'timeSeriesTagsToGroup') ORDER BY name;

DROP TABLE ts;
DROP TABLE t_data;
DROP TABLE t_tags;
DROP TABLE t_ranges;
