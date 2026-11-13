-- One INSERT feeds several materialized views with different targets. In the query log each
-- table must list its own columns: the source the columns the INSERT writes, each view the
-- columns its SELECT produces, each target its insertable columns. The names of the columns may
-- differ between the source, the view and the target.

-- With async inserts the INSERT's own row in the query log is written before the data is
-- processed, and the tables and columns are recorded on the row of the flush instead.
SET async_insert = 0;

DROP TABLE IF EXISTS mv_a;
DROP TABLE IF EXISTS mv_b;
DROP TABLE IF EXISTS mv_all;
DROP TABLE IF EXISTS mv_renamed;
DROP TABLE IF EXISTS mv_inner;
DROP TABLE IF EXISTS tgt_a;
DROP TABLE IF EXISTS tgt_b;
DROP TABLE IF EXISTS tgt_all;
DROP TABLE IF EXISTS tgt_renamed;
DROP TABLE IF EXISTS src;

CREATE TABLE src (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE tgt_a (a UInt32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE tgt_b (b String) ENGINE = MergeTree ORDER BY b;
CREATE TABLE tgt_all (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
-- Other names than in the source
CREATE TABLE tgt_renamed (tgtKey UInt32, tgtVal String) ENGINE = MergeTree ORDER BY tgtKey;

CREATE MATERIALIZED VIEW mv_a TO tgt_a AS SELECT a FROM src;
CREATE MATERIALIZED VIEW mv_b TO tgt_b AS SELECT b FROM src;
CREATE MATERIALIZED VIEW mv_all TO tgt_all AS SELECT * FROM src;
CREATE MATERIALIZED VIEW mv_renamed TO tgt_renamed AS SELECT a AS tgtKey, b AS tgtVal FROM src;
-- A view with its own inner table
CREATE MATERIALIZED VIEW mv_inner ENGINE = MergeTree ORDER BY a AS SELECT a, length(b) AS len FROM src;

INSERT INTO src SETTINGS log_comment = '05621_fan_out' VALUES (1, 'x'), (2, 'yy');

SELECT 'tgt_a', * FROM tgt_a ORDER BY a;
SELECT 'tgt_b', * FROM tgt_b ORDER BY b;
SELECT 'tgt_all', * FROM tgt_all ORDER BY a;
SELECT 'tgt_renamed', * FROM tgt_renamed ORDER BY tgtKey;
SELECT 'mv_inner', * FROM mv_inner ORDER BY a;

SYSTEM FLUSH LOGS query_log;

-- The inner table of mv_inner has a random name, so it is replaced with a placeholder
SELECT
    arrayMap(x -> replaceRegexpOne(x, '`\\.inner_id\\.[0-9a-f-]+`', 'INNER'), tables) AS tables,
    views,
    arrayMap(x -> replaceRegexpOne(x, '`\\.inner_id\\.[0-9a-f-]+`', 'INNER'), columns) AS columns
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND log_comment = '05621_fan_out'
FORMAT Vertical;

DROP TABLE mv_a;
DROP TABLE mv_b;
DROP TABLE mv_all;
DROP TABLE mv_renamed;
DROP TABLE mv_inner;
DROP TABLE tgt_a;
DROP TABLE tgt_b;
DROP TABLE tgt_all;
DROP TABLE tgt_renamed;
DROP TABLE src;
