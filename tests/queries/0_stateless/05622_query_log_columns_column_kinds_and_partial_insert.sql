-- Column kinds and partial inserts. In the query log the inserted table must list exactly the
-- columns the statement writes, a view the columns its SELECT produces, and a target its
-- insertable columns, whatever other kinds of columns (MATERIALIZED, EPHEMERAL, ALIAS, DEFAULT)
-- the tables have.

-- With async inserts the INSERT's own row in the query log is written before the data is
-- processed, and the tables and columns are recorded on the row of the flush instead.
SET async_insert = 0;

DROP TABLE IF EXISTS mv_kinds;
DROP TABLE IF EXISTS mv_plain;
DROP TABLE IF EXISTS mv_three;
DROP TABLE IF EXISTS tgt_kinds;
DROP TABLE IF EXISTS tgt_wide;
DROP TABLE IF EXISTS tgt_three;
DROP TABLE IF EXISTS src_kinds;
DROP TABLE IF EXISTS src_plain;
DROP TABLE IF EXISTS src_three;

-- The source has every kind of column; the view reads a DEFAULT and a MATERIALIZED column
CREATE TABLE src_kinds (a UInt64, e UInt64 EPHEMERAL, d UInt64 DEFAULT e * 10, m UInt64 MATERIALIZED a + 1, al UInt64 ALIAS a + 2) ENGINE = MergeTree ORDER BY a;
CREATE TABLE tgt_kinds (a UInt64, d UInt64, m UInt64) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv_kinds TO tgt_kinds AS SELECT a, d, m FROM src_kinds;
INSERT INTO src_kinds (a, e) SETTINGS log_comment = '05622_source_column_kinds' VALUES (1, 2);
SELECT 'tgt_kinds', * FROM tgt_kinds;

-- The target has every kind of column; the view writes only a
CREATE TABLE src_plain (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE tgt_wide (a UInt64, m UInt64 MATERIALIZED a * 2, e UInt64 EPHEMERAL, d UInt64 DEFAULT e + 1, al UInt64 ALIAS a + 3) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv_plain TO tgt_wide AS SELECT a FROM src_plain;
INSERT INTO src_plain SETTINGS log_comment = '05622_target_column_kinds' VALUES (5);
SELECT 'tgt_wide', a, m, d, al FROM tgt_wide;

-- Three columns, the INSERT writes the first and the third
CREATE TABLE src_three (a UInt64, b String DEFAULT 'def', c UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE tgt_three (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv_three TO tgt_three AS SELECT * FROM src_three;
INSERT INTO src_three (a, c) SETTINGS log_comment = '05622_first_and_third_values' VALUES (1, 3);
INSERT INTO src_three (a, c) SETTINGS log_comment = '05622_first_and_third_select' SELECT 2, 4;
SELECT 'tgt_three', * FROM tgt_three ORDER BY a;

-- The same partial INSERT into a table without views: only the two written columns are listed
INSERT INTO tgt_three (a, c) SETTINGS log_comment = '05622_first_and_third_no_views' VALUES (3, 5);

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, tables, views, columns
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND startsWith(log_comment, '05622_')
ORDER BY log_comment
FORMAT Vertical;

DROP TABLE mv_kinds;
DROP TABLE mv_plain;
DROP TABLE mv_three;
DROP TABLE tgt_kinds;
DROP TABLE tgt_wide;
DROP TABLE tgt_three;
DROP TABLE src_kinds;
DROP TABLE src_plain;
DROP TABLE src_three;
