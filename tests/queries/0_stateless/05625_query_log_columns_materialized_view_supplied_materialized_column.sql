-- The target of a view gets the columns the view supplies, converted by name. A supplied MATERIALIZED column is
-- written and listed; a DEFAULT or EPHEMERAL column nobody supplies is computed by the target and not listed.
SET async_insert = 0;

DROP TABLE IF EXISTS mv_declared;
DROP TABLE IF EXISTS mv_plain;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dst (a UInt64, m UInt64 MATERIALIZED a + 1, d UInt64 DEFAULT 42) ENGINE = MergeTree ORDER BY a;

-- The view declares `m` as an ordinary column, so an insert into the view may name it, and the value reaches `dst.m`
CREATE MATERIALIZED VIEW mv_declared TO dst (a UInt64, m UInt64) AS SELECT a FROM src;
INSERT INTO mv_declared (a, m) SETTINGS log_comment = '05625_insert_into_view' VALUES (7, 70);

-- Inserting into `src` runs both views. Each supplies only `a`, so the declared `m` of `mv_declared` is not listed,
-- and the target computes `m` and `d`.
CREATE MATERIALIZED VIEW mv_plain TO dst AS SELECT a + 100 AS a FROM src;
INSERT INTO src SETTINGS log_comment = '05625_insert_into_source' VALUES (1);

SELECT 'dst', a, m, d FROM dst ORDER BY a, m;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, columns
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment IN ('05625_insert_into_view', '05625_insert_into_source')
  AND type = 'QueryFinish'
ORDER BY log_comment
FORMAT Vertical;

DROP TABLE mv_declared;
DROP TABLE mv_plain;
DROP TABLE src;
DROP TABLE dst;
