-- The inner query of a SQL SECURITY DEFINER / NONE view runs under the definer's identity, or without one.
-- For a sealed view, what it accesses is logged with the view in system.query_views_log (user = the definer) and not
-- in the caller's query_log row; a transparent view (a plain projection over one MergeTree table, see isSealed) is
-- attributed to the caller's row. system.user_query_log shows the caller's own top-level rows.

SET enable_parallel_replicas = 0;
SET log_queries = 1;
SET log_query_views = 1;
SET optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p_by_name';

CREATE TABLE t_private (id UInt64, name String, PROJECTION p_by_name (SELECT sum(id) GROUP BY name)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_private VALUES (1, 'a'), (2, 'b'), (3, 'a');

CREATE VIEW v_transparent DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id, name FROM t_private;
CREATE VIEW v_none SQL SECURITY NONE AS SELECT id, name FROM t_private;
CREATE VIEW v_invoker SQL SECURITY INVOKER AS SELECT id, name FROM t_private;
-- A WHERE makes a view sealed.
CREATE VIEW v_sealed DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id, name FROM t_private WHERE id > 0;
CREATE VIEW v_none_sealed SQL SECURITY NONE AS SELECT id, name FROM t_private WHERE id > 0;

SELECT name FROM t_private GROUP BY name ORDER BY name SETTINGS log_comment = 'direct' FORMAT Null;
SELECT name FROM v_transparent GROUP BY name ORDER BY name SETTINGS log_comment = 'transparent' FORMAT Null;
SELECT name FROM v_transparent GROUP BY name ORDER BY name SETTINGS log_comment = 'transparent_inlined', analyzer_inline_views = 1 FORMAT Null;
SELECT name FROM v_none GROUP BY name ORDER BY name SETTINGS log_comment = 'none' FORMAT Null;
SELECT name FROM v_invoker GROUP BY name ORDER BY name SETTINGS log_comment = 'invoker' FORMAT Null;

SET force_optimize_projection = 0, force_optimize_projection_name = '';
SELECT name FROM v_sealed ORDER BY name SETTINGS log_comment = 'sealed' FORMAT Null;
SELECT name FROM v_none_sealed ORDER BY name SETTINGS log_comment = 'none_sealed' FORMAT Null;

SYSTEM FLUSH LOGS query_log, query_views_log;

-- The caller's own rows: transparent bodies are attributed here, sealed ones are not.
SELECT
    log_comment,
    has(tables, currentDatabase() || '.t_private') AS table_attributed,
    has(projections, currentDatabase() || '.t_private.p_by_name') AS projection_attributed
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment IN ('direct', 'transparent', 'transparent_inlined', 'none', 'invoker', 'sealed', 'none_sealed')
ORDER BY log_comment;

-- A sealed body is logged with its view.
SELECT
    q.log_comment,
    replaceOne(v.view_name, currentDatabase() || '.', '') AS view_name,
    v.view_type,
    v.user,
    replaceAll(v.view_query, currentDatabase() || '.', '') AS view_query,
    has(v.tables, currentDatabase() || '.t_private') AS table_attributed
FROM system.query_views_log AS v
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
        AND log_comment IN ('direct', 'transparent', 'transparent_inlined', 'none', 'invoker', 'sealed', 'none_sealed')
) AS q ON v.initial_query_id = q.query_id
ORDER BY q.log_comment;

-- The caller's own log shows the top-level rows.
SELECT log_comment, count()
FROM system.user_query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('direct', 'transparent', 'transparent_inlined', 'none', 'invoker', 'sealed', 'none_sealed')
GROUP BY log_comment
ORDER BY log_comment;
