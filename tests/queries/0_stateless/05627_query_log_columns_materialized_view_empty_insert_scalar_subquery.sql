-- An insert that moves no rows does not run the SELECT of a view. The log still lists the columns the view writes,
-- found by analysis alone: a scalar subquery of the view is not executed, so one that would fail does not fail here.
SET async_insert = 0;

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS lookup;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE lookup (b UInt64) ENGINE = MergeTree ORDER BY b;
CREATE TABLE dst (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;

-- Two rows, so the scalar subquery fails when it runs
INSERT INTO lookup VALUES (1), (2);

CREATE MATERIALIZED VIEW mv TO dst AS SELECT a, (SELECT b FROM lookup) AS c FROM src;

INSERT INTO src SETTINGS log_comment = '05627_empty_insert' SELECT 1 WHERE 0;
INSERT INTO src VALUES (1); -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }

SELECT 'dst', count() FROM dst;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, columns
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '05627_empty_insert'
  AND type = 'QueryFinish'
FORMAT Vertical;

DROP TABLE mv;
DROP TABLE src;
DROP TABLE lookup;
DROP TABLE dst;
