-- Inline data and `INSERT INTO FUNCTION` must use the same attribution as `INSERT SELECT`.
-- Query comments and the current database isolate the metrics from concurrent inserts.

SET async_insert = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET parallel_view_processing = 0;

DROP VIEW IF EXISTS values_metrics_mv;
DROP TABLE IF EXISTS values_metrics_src;
DROP TABLE IF EXISTS values_metrics_dst;

CREATE TABLE values_metrics_src (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE values_metrics_dst (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW values_metrics_mv TO values_metrics_dst AS SELECT id, s FROM values_metrics_src;

INSERT INTO /* test 05617 direct */ values_metrics_dst VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
INSERT INTO /* test 05617 mv */ values_metrics_src VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
INSERT INTO /* test 05617 function */ FUNCTION null('id UInt64, s String') VALUES (1, 'a'), (2, 'b'), (3, 'c');

INSERT INTO /* test 05617 function_select */ FUNCTION null('id UInt64, s String') SELECT number, toString(number) FROM numbers(7);

SYSTEM FLUSH LOGS query_log;

-- A direct `INSERT ... VALUES` attributes all five rows to `DirectInsertedRows`.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['DirectInsertedBytes'] > 0,
    ProfileEvents['MaterializedViewInsertedBytes'],
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 05617 direct */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

-- `INSERT ... VALUES` writes four rows directly and four rows through the materialized view.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['DirectInsertedBytes'] > 0,
    ProfileEvents['MaterializedViewInsertedBytes'] > 0,
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 05617 mv */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

-- A table function attributes all three inline rows to `DirectInsertedRows`.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['DirectInsertedBytes'] > 0,
    ProfileEvents['MaterializedViewInsertedBytes'],
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 05617 function */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

-- `INSERT SELECT` uses the same direct counters for a table function.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['DirectInsertedBytes'] > 0,
    ProfileEvents['MaterializedViewInsertedBytes'],
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 05617 function_select */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

DROP VIEW values_metrics_mv;
DROP TABLE values_metrics_src;
DROP TABLE values_metrics_dst;
