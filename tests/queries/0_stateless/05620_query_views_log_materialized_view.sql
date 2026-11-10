-- A materialized view with SQL SECURITY DEFINER / NONE runs under the definer's identity, both when an insert pushes
-- rows through it and when its target is read through it. What it accesses is logged with the view in
-- system.query_views_log (user = the definer) and not in the caller's query_log row. A materialized view without a
-- SQL SECURITY clause runs as the caller: it stays in the caller's row, and its query_views_log row has no access columns.

SET enable_parallel_replicas = 0;
SET log_queries = 1;
SET log_query_views = 1;

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE lookup_definer (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE lookup_legacy (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup_definer SELECT number, toString(number) FROM numbers(3);
INSERT INTO lookup_legacy SELECT number, toString(number) FROM numbers(3);
CREATE TABLE dst_definer (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_legacy (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_definer TO dst_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT s.id, l.v FROM src AS s JOIN lookup_definer AS l ON s.id = l.id;
CREATE MATERIALIZED VIEW mv_legacy TO dst_legacy AS
    SELECT s.id, l.v FROM src AS s JOIN lookup_legacy AS l ON s.id = l.id;

INSERT INTO src SETTINGS log_comment = 'insert' SELECT number FROM numbers(2);

-- Read rows, not an aggregate: an aggregate may be answered from part statistics without reading any part.
SELECT id FROM dst_definer ORDER BY id SETTINGS log_comment = 'read_target' FORMAT Null;
SELECT id FROM mv_definer ORDER BY id SETTINGS log_comment = 'read_definer' FORMAT Null;
SELECT id FROM mv_legacy ORDER BY id SETTINGS log_comment = 'read_legacy' FORMAT Null;

SYSTEM FLUSH LOGS query_log, query_views_log;

-- The caller's own rows.
SELECT
    log_comment,
    arrayExists(x -> startsWith(x, currentDatabase() || '.dst_definer') OR startsWith(x, currentDatabase() || '.lookup_definer'), arrayConcat(tables, partitions)) AS definer_objects,
    has(views, currentDatabase() || '.mv_definer') AS definer_view,
    arrayExists(x -> startsWith(x, currentDatabase() || '.dst_legacy') OR startsWith(x, currentDatabase() || '.lookup_legacy'), arrayConcat(tables, partitions)) AS legacy_objects,
    has(views, currentDatabase() || '.mv_legacy') AS legacy_view
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment IN ('insert', 'read_target', 'read_definer', 'read_legacy')
ORDER BY log_comment;

-- The views' rows: the definer view's work on insert and on read, the legacy view's row on insert only.
SELECT
    q.log_comment,
    replaceOne(v.view_name, currentDatabase() || '.', '') AS view_name,
    v.view_type,
    replaceOne(v.view_target, currentDatabase() || '.', '') AS view_target,
    v.user,
    replaceAll(v.view_query, currentDatabase() || '.', '') AS view_query,
    arrayExists(x -> startsWith(x, currentDatabase() || '.dst_definer'), arrayConcat(v.tables, v.partitions)) AS target,
    has(v.tables, currentDatabase() || '.lookup_definer') AS lookup
FROM system.query_views_log AS v
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
        AND log_comment IN ('insert', 'read_target', 'read_definer', 'read_legacy')
) AS q ON v.initial_query_id = q.query_id
ORDER BY q.log_comment, view_name;

-- The caller's own log shows the top-level rows.
SELECT log_comment, count()
FROM system.user_query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('insert', 'read_target', 'read_definer', 'read_legacy')
GROUP BY log_comment
ORDER BY log_comment;
