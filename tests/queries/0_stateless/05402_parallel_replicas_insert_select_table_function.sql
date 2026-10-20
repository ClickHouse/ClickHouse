-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- `INSERT SELECT` with `parallel_distributed_insert_select = 2` reading through a `TimeSeries` table function.
-- The test database is not the replicas' default database, so the call sent to them has to name it.
-- The destination is a `Null` table with a materialized view, so each replica's share of the rows is counted.

SET allow_experimental_time_series_table = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_distributed_insert_select = 2;
SET automatic_parallel_replicas_mode = 0;

CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES ('m1', {'job': 'j1'}, [(1, 1.), (2, 2.)]), ('m2', {'job': 'j2'}, [(1, 3.)]);

CREATE TABLE dst (value Float64) ENGINE = Null;
CREATE TABLE res (value Float64) ENGINE = MergeTree ORDER BY value;
CREATE MATERIALIZED VIEW dst_mv TO res AS SELECT value FROM dst;

SELECT '-- local plan';
INSERT INTO dst SELECT value FROM timeSeriesSamples(ts) SETTINGS parallel_replicas_local_plan = 1;
SELECT count(), sum(value) FROM res;
TRUNCATE TABLE res;

SELECT '-- no local plan';
INSERT INTO dst SELECT value FROM timeSeriesSamples(ts) SETTINGS parallel_replicas_local_plan = 0, log_comment = '05402_no_local_plan';
SELECT count(), sum(value) FROM res;

-- The insert has to have been sent to the other replicas, not run on the initiator alone.
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND NOT is_initial_query AND event_date >= yesterday()
  AND initial_query_id = (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
      AND log_comment = '05402_no_local_plan' AND event_date >= yesterday()
    ORDER BY event_time_microseconds DESC LIMIT 1);

DROP TABLE dst_mv;
DROP TABLE res;
DROP TABLE dst;
DROP TABLE ts;
