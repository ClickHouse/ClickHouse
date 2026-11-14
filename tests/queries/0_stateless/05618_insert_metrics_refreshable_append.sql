-- Tags: no-replicated-database
-- The refresh can execute on another replica, but `system.query_log` is local.
-- An `APPEND` refresh writes directly to its target and feeds the target's dependent view.

SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET parallel_view_processing = 0;
SET async_insert = 0;

DROP TABLE IF EXISTS rmv_metrics_rmv;
DROP TABLE IF EXISTS rmv_metrics_mv;
DROP TABLE IF EXISTS rmv_metrics_target;
DROP TABLE IF EXISTS rmv_metrics_downstream;

CREATE TABLE rmv_metrics_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE rmv_metrics_downstream (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW rmv_metrics_mv TO rmv_metrics_downstream AS SELECT x FROM rmv_metrics_target WHERE x % 2 = 0;

CREATE MATERIALIZED VIEW rmv_metrics_rmv REFRESH EVERY 1 YEAR APPEND TO rmv_metrics_target AS SELECT number AS x FROM numbers(5);
SYSTEM WAIT VIEW rmv_metrics_rmv;

SELECT count() FROM rmv_metrics_target;
SELECT count() FROM rmv_metrics_downstream;

SYSTEM FLUSH LOGS query_log;

-- The refresh logs its own query with the database and view names in `log_comment`.
-- Five rows reach the target directly. The dependent view writes three even values.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment LIKE 'refresh of ' || currentDatabase() || '.rmv_metrics_rmv%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE rmv_metrics_rmv;
DROP TABLE rmv_metrics_mv;
DROP TABLE rmv_metrics_target;
DROP TABLE rmv_metrics_downstream;
