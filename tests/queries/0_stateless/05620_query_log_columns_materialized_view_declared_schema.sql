-- A `TO` materialized view may declare more columns than its SELECT produces, and the view's own
-- DEFAULT expressions are never evaluated: the target fills such a column from its own default.
-- The query log must list, for the view, only the columns its SELECT produces.

-- With async inserts the INSERT's own row in the query log is written before the data is
-- processed, and the tables and columns are recorded on the row of the flush instead.
SET async_insert = 0;

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS src;

CREATE TABLE src (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dst (a UInt64, extra UInt64 DEFAULT 42) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv TO dst (a UInt64, extra UInt64 DEFAULT 0) AS SELECT a FROM src;

INSERT INTO src SETTINGS log_comment = '05620_declared_view_schema' VALUES (1);

-- The default of the target is used, not the default of the view
SELECT * FROM dst;

SYSTEM FLUSH LOGS query_log;

-- `mv.extra` must not be listed: the view's SELECT produces only `a`
SELECT tables, views, columns
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND log_comment = '05620_declared_view_schema'
FORMAT Vertical;

DROP TABLE mv;
DROP TABLE dst;
DROP TABLE src;
