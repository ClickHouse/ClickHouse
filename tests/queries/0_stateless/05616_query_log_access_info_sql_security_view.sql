-- What the body of a SQL SECURITY DEFINER / NONE view runs under its identity is logged as a child row of the caller's
-- query; the caller's own row keeps only what it ran itself, and system.user_query_log shows no child rows.

SET enable_parallel_replicas = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_START';
SET optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p_by_name';

CREATE TABLE t_private (id UInt64, name String, PROJECTION p_by_name (SELECT name, sum(id) GROUP BY name)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_private VALUES (1, 'a'), (2, 'b'), (3, 'a');

CREATE VIEW v_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id, name FROM t_private;
CREATE VIEW v_invoker SQL SECURITY INVOKER AS SELECT id, name FROM t_private;
CREATE VIEW v_sealed DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT name, sum(id) AS s FROM t_private GROUP BY name;
CREATE VIEW v_none SQL SECURITY NONE AS SELECT name, sum(id) AS s FROM t_private GROUP BY name;
CREATE VIEW v_nested DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT name FROM v_sealed WHERE s > 0;
CREATE VIEW v_param DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT name, sum(id) AS s FROM t_private WHERE id > {p:UInt64} GROUP BY name;
CREATE TABLE t_policy (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_policy VALUES (1), (2);
CREATE ROW POLICY p_policy ON t_policy USING id > 1 TO ALL;
CREATE VIEW v_policy DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id FROM t_policy;

SELECT name FROM t_private GROUP BY name ORDER BY name SETTINGS log_comment = 'direct' FORMAT Null;
SELECT name FROM v_definer GROUP BY name ORDER BY name SETTINGS log_comment = 'definer' FORMAT Null;
SELECT name FROM v_definer GROUP BY name ORDER BY name SETTINGS log_comment = 'definer_inlined', analyzer_inline_views = 1 FORMAT Null;
SELECT name FROM v_invoker GROUP BY name ORDER BY name SETTINGS log_comment = 'invoker' FORMAT Null;
SELECT name FROM v_sealed ORDER BY name SETTINGS log_comment = 'sealed' FORMAT Null;
SELECT name FROM v_none ORDER BY name SETTINGS log_comment = 'none' FORMAT Null;
SELECT name FROM v_nested ORDER BY name SETTINGS log_comment = 'nested' FORMAT Null;
SELECT a.name FROM v_sealed AS a JOIN v_sealed AS b USING name ORDER BY a.name SETTINGS log_comment = 'sealed_twice' FORMAT Null;

SET force_optimize_projection = 0, force_optimize_projection_name = '';

-- A filtered aggregate need not use the projection.
SELECT name FROM v_param(p = 0) UNION ALL SELECT name FROM v_param(p = 1) SETTINGS log_comment = 'param' FORMAT Null;

-- The body's own optimization fails while the caller is still planning, after the body was registered.
SELECT name FROM v_sealed SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_missing', log_comment = 'before_start' FORMAT Null; -- { serverError INCORRECT_DATA }
SELECT throwIf(s > 0) FROM v_sealed SETTINGS log_comment = 'while_processing' FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT id FROM v_policy SETTINGS log_comment = 'policy' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'caller rows';
SELECT
    log_comment,
    has(tables, currentDatabase() || '.t_private') AS reads_table,
    has(projections, currentDatabase() || '.t_private.p_by_name') AS uses_projection
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type IN ('QueryFinish', 'ExceptionBeforeStart')
    AND log_comment IN ('direct', 'definer', 'definer_inlined', 'invoker', 'sealed', 'none', 'nested', 'sealed_twice', 'param', 'before_start')
ORDER BY log_comment;

SELECT 'child rows';
SELECT
    log_comment,
    replaceAll(query, currentDatabase() || '.', '') AS body,
    multiIf(user = currentUser(), 'definer', user = '', 'none', 'other') AS identity,
    has(tables, currentDatabase() || '.t_private') AS reads_table,
    has(projections, currentDatabase() || '.t_private.p_by_name') AS uses_projection,
    initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase() AND is_initial_query AND type IN ('QueryFinish', 'ExceptionBeforeStart')
            AND log_comment IN ('direct', 'definer', 'definer_inlined', 'invoker', 'sealed', 'none', 'nested', 'sealed_twice', 'param', 'before_start')
    ) AS of_caller
FROM system.query_log
WHERE current_database = currentDatabase() AND NOT is_initial_query AND type = 'QueryFinish'
    AND log_comment IN ('direct', 'definer', 'definer_inlined', 'invoker', 'sealed', 'none', 'nested', 'sealed_twice', 'param')
ORDER BY log_comment, body;

SELECT 'user_query_log';
SELECT log_comment, countIf(type = 'QueryStart'), countIf(type = 'QueryFinish'), countIf(type = 'ExceptionBeforeStart')
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('direct', 'definer', 'definer_inlined', 'invoker', 'sealed', 'none', 'nested', 'sealed_twice', 'param', 'before_start')
GROUP BY log_comment
ORDER BY log_comment;

SELECT 'failed query child rows';
SELECT
    log_comment,
    type,
    multiIf(user = currentUser(), 'definer', user = '', 'none', 'other') AS identity,
    has(tables, currentDatabase() || '.t_private') AS reads_table
FROM system.query_log
WHERE current_database = currentDatabase() AND NOT is_initial_query AND log_comment IN ('before_start', 'while_processing')
ORDER BY log_comment;

SELECT 'row policies';
SELECT is_initial_query, length(used_row_policies)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'policy'
ORDER BY is_initial_query;

DROP ROW POLICY p_policy ON t_policy;
