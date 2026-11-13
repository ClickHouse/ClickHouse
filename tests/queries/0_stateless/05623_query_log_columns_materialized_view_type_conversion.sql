-- The SELECT of a materialized view may produce a narrower type than the target column has.
-- The rows are converted to the target type, and the query log lists the columns of the
-- source, the view and the target.

-- With async inserts the INSERT's own row in the query log is written before the data is
-- processed, and the tables and columns are recorded on the row of the flush instead.
SET async_insert = 0;

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS tgt;
DROP TABLE IF EXISTS src;

CREATE TABLE src (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE tgt (tgtKey UInt64, tgtVal String) ENGINE = MergeTree ORDER BY tgtKey;
CREATE MATERIALIZED VIEW mv TO tgt AS SELECT a AS tgtKey, b AS tgtVal FROM src;

INSERT INTO src SETTINGS log_comment = '05623_type_conversion' VALUES (1, 'x');

SELECT *, toTypeName(tgtKey) FROM tgt;

SYSTEM FLUSH LOGS query_log;

SELECT tables, views, columns
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND log_comment = '05623_type_conversion'
FORMAT Vertical;

DROP TABLE mv;
DROP TABLE tgt;
DROP TABLE src;
