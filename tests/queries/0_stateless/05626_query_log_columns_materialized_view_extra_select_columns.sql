-- With `allow_materialized_view_with_bad_select`, the SELECT of a view may produce columns its target lacks, and an
-- insert into a view may supply declared columns its target lacks. The conversion drops them silently, so the log
-- lists for the target only the columns it stores.
SET async_insert = 0;
SET allow_materialized_view_with_bad_select = 1;

DROP TABLE IF EXISTS mv_extra;
DROP TABLE IF EXISTS mv_direct;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (v1 UInt64, v2 UInt64) ENGINE = MergeTree ORDER BY v1;
CREATE TABLE dst (v UInt64) ENGINE = MergeTree ORDER BY v;

-- The SELECT produces `v2`, which `dst` lacks
CREATE MATERIALIZED VIEW mv_extra TO dst AS SELECT v1 AS v, v2 FROM src;
INSERT INTO src SETTINGS log_comment = '05626_insert_into_source' VALUES (1, 10);

-- The view declares `extra`, which `dst` lacks, and the insert supplies it
CREATE MATERIALIZED VIEW mv_direct TO dst (v UInt64, extra UInt64) AS SELECT v1 AS v FROM src;
INSERT INTO mv_direct (v, extra) SETTINGS log_comment = '05626_insert_into_view' VALUES (2, 20);

SELECT 'dst', v FROM dst ORDER BY v;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, columns
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment IN ('05626_insert_into_source', '05626_insert_into_view')
  AND type = 'QueryFinish'
ORDER BY log_comment
FORMAT Vertical;

DROP TABLE mv_extra;
DROP TABLE mv_direct;
DROP TABLE src;
DROP TABLE dst;
