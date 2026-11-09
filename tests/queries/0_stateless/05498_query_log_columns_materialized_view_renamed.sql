-- Every table that receives data from an INSERT must be recorded in system.query_log.columns
-- with its own column names: the table named in the statement with the columns it writes,
-- each materialized view with the columns its SELECT produces, and each view target with its
-- insertable columns.

-- With asynchronous inserts the row of an INSERT with inline data is written before the data is
-- flushed, so the columns would be recorded on the flush row instead.
SET async_insert = 0;

DROP TABLE IF EXISTS mv_second;
DROP TABLE IF EXISTS mv_first;
DROP TABLE IF EXISTS dst_second;
DROP TABLE IF EXISTS dst_first;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS wide;

CREATE TABLE src (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dst_first (b UInt64) ENGINE = MergeTree ORDER BY b;
CREATE TABLE dst_second (c UInt64) ENGINE = MergeTree ORDER BY c;

-- The source, the view and the target all use different column names.
CREATE MATERIALIZED VIEW mv_first TO dst_first AS SELECT a AS b FROM src;
-- A cascaded view renames the column once more.
CREATE MATERIALIZED VIEW mv_second TO dst_second AS SELECT b AS c FROM dst_first;

INSERT INTO src SETTINGS log_comment = '05498_renamed_through_views' VALUES (1), (2), (3);

SELECT * FROM dst_second ORDER BY c;

-- Only the columns the statement writes are recorded for the target table.
CREATE TABLE wide (a UInt64, b String, c UInt64 MATERIALIZED a * 2) ENGINE = MergeTree ORDER BY a;
INSERT INTO wide (a) SETTINGS log_comment = '05498_explicit_column_list' VALUES (1);
-- Without a column list the DEFAULT columns are recorded, the MATERIALIZED one is not.
INSERT INTO wide SETTINGS log_comment = '05498_no_column_list' VALUES (2, 'x');

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, tables, views, columns
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND startsWith(log_comment, '05498_')
ORDER BY log_comment
FORMAT Vertical;

DROP TABLE mv_second;
DROP TABLE mv_first;
DROP TABLE dst_second;
DROP TABLE dst_first;
DROP TABLE src;
DROP TABLE wide;
