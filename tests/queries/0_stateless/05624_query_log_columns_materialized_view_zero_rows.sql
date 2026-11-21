-- The columns in the query log describe the statement, not the data: an insert that moves no rows lists the columns
-- of the inserted table too. The columns a materialized view writes into its target are known only when its SELECT
-- is analyzed, which happens with the first block, so an insert that moves no rows lists the target by name only, as
-- the QueryStart row does.
SET async_insert = 0;

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS tgt;
DROP TABLE IF EXISTS empty_src;

CREATE TABLE src (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE tgt (tgtKey UInt64, tgtVal String) ENGINE = MergeTree ORDER BY tgtKey;
CREATE TABLE empty_src (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv TO tgt AS SELECT a AS tgtKey, b AS tgtVal FROM src;

INSERT INTO src SETTINGS log_comment = '05624_zero_rows_where' SELECT 1, 'x' WHERE 0;
INSERT INTO src SETTINGS log_comment = '05624_zero_rows_empty_table' SELECT * FROM empty_src;
INSERT INTO src SETTINGS log_comment = '05624_one_row' VALUES (1, 'x');

SELECT 'rows in tgt', count() FROM tgt;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, type, views, columns
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment IN ('05624_zero_rows_where', '05624_zero_rows_empty_table', '05624_one_row')
  AND type IN ('QueryStart', 'QueryFinish')
ORDER BY log_comment, type
FORMAT Vertical;

DROP TABLE mv;
DROP TABLE src;
DROP TABLE tgt;
DROP TABLE empty_src;
