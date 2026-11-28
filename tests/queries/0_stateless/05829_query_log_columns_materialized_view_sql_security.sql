-- A materialized view with `SQL SECURITY DEFINER` or `NONE` runs under another identity, and its work is logged on a
-- child row of the query, not on the user's row. The columns a view writes into its target go to the same row as the
-- tables the view reads: the user's row for a plain view, the child row for a DEFINER or NONE view.
SET async_insert = 0;
SET log_queries = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS mv_definer;
DROP TABLE IF EXISTS mv_legacy;
DROP TABLE IF EXISTS mv_none;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS lookup;
DROP TABLE IF EXISTS dst_definer;
DROP TABLE IF EXISTS dst_legacy;
DROP TABLE IF EXISTS dst_none;

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE lookup (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_definer (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_legacy (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_none (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup VALUES (1, 'one');

CREATE MATERIALIZED VIEW mv_definer TO dst_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER
    AS SELECT s.id AS id, l.v AS v FROM src AS s LEFT JOIN lookup AS l ON s.id = l.id;
CREATE MATERIALIZED VIEW mv_legacy TO dst_legacy AS SELECT id FROM src;
CREATE MATERIALIZED VIEW mv_none TO dst_none SQL SECURITY NONE AS SELECT id FROM src;

INSERT INTO src SETTINGS log_comment = '05829_insert' VALUES (1);

-- The view is the table of the statement, so the user's row lists it with the columns of the statement. Its target
-- is written under the identity of the definer and listed on the child row.
INSERT INTO mv_definer (id, v) SETTINGS log_comment = '05829_insert_into_view' VALUES (2, 'two');

SELECT 'dst_definer', * FROM dst_definer ORDER BY id;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    if(is_initial_query, 'caller', replaceAll(query, currentDatabase() || '.', '')) AS body,
    multiIf(is_initial_query, 'user', user = '', 'none', user = currentUser(), 'definer', 'other') AS identity,
    arrayStringConcat(arrayMap(x -> replaceAll(x, currentDatabase() || '.', ''), tables), ',') AS tables,
    arrayStringConcat(arrayMap(x -> replaceAll(x, currentDatabase() || '.', ''), views), ',') AS views,
    arrayStringConcat(arrayMap(x -> replaceAll(x, currentDatabase() || '.', ''), columns), ',') AS columns
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment IN ('05829_insert', '05829_insert_into_view')
ORDER BY log_comment, is_initial_query DESC, body
FORMAT Vertical;

DROP TABLE mv_definer;
DROP TABLE mv_legacy;
DROP TABLE mv_none;
DROP TABLE src;
DROP TABLE lookup;
DROP TABLE dst_definer;
DROP TABLE dst_legacy;
DROP TABLE dst_none;
