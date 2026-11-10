-- The body of a SQL SECURITY DEFINER / NONE view runs under the definer's identity or without one. What it reads is
-- attributed to the caller's query_log row, which is marked as hidden, and the caller's own system.user_query_log
-- omits that row. Relies on query_log.enable_query_access_info_attribution = 1 from tests/config.

SET enable_parallel_replicas = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_START';
SET optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p_by_name';

CREATE TABLE t_private (id UInt64, name String, PROJECTION p_by_name (SELECT sum(id) GROUP BY name)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_private VALUES (1, 'a'), (2, 'b'), (3, 'a');

CREATE VIEW v_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id, name FROM t_private;
CREATE VIEW v_none SQL SECURITY NONE AS SELECT id, name FROM t_private;
CREATE VIEW v_invoker SQL SECURITY INVOKER AS SELECT id, name FROM t_private;

SELECT name FROM t_private GROUP BY name ORDER BY name SETTINGS log_comment = 'direct' FORMAT Null;
SELECT name FROM v_definer GROUP BY name ORDER BY name SETTINGS log_comment = 'definer' FORMAT Null;
SELECT name FROM v_definer GROUP BY name ORDER BY name SETTINGS log_comment = 'definer_inlined', analyzer_inline_views = 1 FORMAT Null;
SELECT name FROM v_none GROUP BY name ORDER BY name SETTINGS log_comment = 'none' FORMAT Null;
SELECT name FROM v_invoker GROUP BY name ORDER BY name SETTINGS log_comment = 'invoker' FORMAT Null;

SET force_optimize_projection = 0, force_optimize_projection_name = '';
SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    has(tables, currentDatabase() || '.t_private') AS table_attributed,
    has(projections, currentDatabase() || '.t_private.p_by_name') AS projection_attributed,
    query_access_info_is_hidden AS hidden
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('direct', 'definer', 'definer_inlined', 'none', 'invoker')
ORDER BY log_comment;

-- The start row of a hidden query is marked as well.
SELECT log_comment, query_access_info_is_hidden AS hidden
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryStart'
    AND log_comment IN ('direct', 'definer', 'definer_inlined', 'none', 'invoker')
ORDER BY log_comment;

-- The caller's own log keeps only the rows that are not marked.
SELECT log_comment, count()
FROM system.user_query_log
WHERE current_database = currentDatabase() AND type IN ('QueryStart', 'QueryFinish')
    AND log_comment IN ('direct', 'definer', 'definer_inlined', 'none', 'invoker')
GROUP BY log_comment
ORDER BY log_comment;
