-- `system.query_views_log` has the settings changed for the insert into the view's target table.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT x FROM src;

INSERT INTO src SETTINGS log_queries = 1, log_query_views = 1, log_queries_min_type = 'QUERY_START', log_queries_min_query_duration_ms = 0,
    log_query_settings = 1, materialize_skip_indexes_on_insert = 0 VALUES (1);
INSERT INTO src SETTINGS log_queries = 1, log_query_views = 1, log_queries_min_type = 'QUERY_START', log_queries_min_query_duration_ms = 0,
    log_query_settings = 0, materialize_skip_indexes_on_insert = 0 VALUES (2);

SYSTEM FLUSH LOGS query_views_log;

SELECT Settings['materialize_skip_indexes_on_insert'] = '0', empty(Settings)
FROM system.query_views_log
WHERE event_date >= yesterday() AND view_name = currentDatabase() || '.mv'
ORDER BY event_time_microseconds;

DROP TABLE mv;
DROP TABLE dst;
DROP TABLE src;
