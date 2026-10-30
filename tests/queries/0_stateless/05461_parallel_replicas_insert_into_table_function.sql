-- `INSERT INTO FUNCTION` with `parallel_distributed_insert_select = 2`, writing through a `TimeSeries` table function.
-- The test database is not the replicas' default database, so the destination sent to them has to name it.
-- The destination's samples table is replicated, which is what lets the insert be distributed.

SET allow_experimental_time_series_table = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_distributed_insert_select = 2;
SET automatic_parallel_replicas_mode = 0;

CREATE TABLE src ENGINE = TimeSeries;
INSERT INTO src (metric_name, tags, samples) VALUES ('m1', {'job': 'j1'}, [(1, 1.), (2, 2.)]), ('m2', {'job': 'j2'}, [(1, 3.)]);

CREATE TABLE dst ENGINE = TimeSeries
    SAMPLES INNER ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{database}/05461_dst_samples', '1');

INSERT INTO FUNCTION timeSeriesSamples(dst) SELECT * FROM timeSeriesSamples(src)
SETTINGS parallel_replicas_local_plan = 0, log_comment = '05461_insert';
SELECT count(), sum(s.2) FROM timeSeriesSamples(dst) ARRAY JOIN samples AS s SETTINGS enable_parallel_replicas = 0;

-- The insert has to have been sent to the other replicas, not run on the initiator alone.
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND NOT is_initial_query AND event_date >= yesterday()
  AND initial_query_id = (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
      AND log_comment = '05461_insert' AND event_date >= yesterday()
    ORDER BY event_time_microseconds DESC LIMIT 1);

DROP TABLE dst;
DROP TABLE src;
