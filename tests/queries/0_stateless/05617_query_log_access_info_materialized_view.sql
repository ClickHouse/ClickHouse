-- What a SQL SECURITY DEFINER materialized view runs under its identity (its query on INSERT, an insert into it, a read
-- through it) is logged as a child row of the caller's query, together with the tables it writes and the views it triggers.

SET enable_parallel_replicas = 0;
SET log_queries = 1;

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE lookup (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup VALUES (0, 'zero'), (1, 'one');
CREATE TABLE dst_definer (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_legacy (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_chain (id UInt64, v String) ENGINE = Memory;
CREATE TABLE dst_none (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE src_dropped (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_dropped (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_no_push (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05617.csv', 'CSV', 'id UInt64') SELECT 1 SETTINGS engine_file_truncate_on_insert = 1;
-- An insert into this engine does not push to its views.
CREATE TABLE src_no_push (id UInt64) ENGINE = FileLog({CLICKHOUSE_DATABASE:String} || '_05617.csv', 'CSV');

-- Sealed, so the materialized view below plans it again for every inserted block.
CREATE VIEW v_lookup DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id, v FROM lookup WHERE id >= 0;
CREATE MATERIALIZED VIEW mv_definer TO dst_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER
    AS SELECT s.id AS id, l.v AS v FROM src AS s LEFT JOIN v_lookup AS l ON s.id = l.id;
CREATE MATERIALIZED VIEW mv_chain TO dst_chain DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id, v FROM dst_definer;
CREATE MATERIALIZED VIEW mv_legacy TO dst_legacy AS SELECT id FROM src;
CREATE MATERIALIZED VIEW mv_none TO dst_none SQL SECURITY NONE AS SELECT id FROM src;
CREATE MATERIALIZED VIEW mv_dropped TO dst_dropped DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id FROM src_dropped;
CREATE MATERIALIZED VIEW mv_no_push TO dst_no_push DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id FROM src_no_push;
DROP TABLE dst_dropped;

INSERT INTO src SETTINGS log_comment = 'insert', max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
SELECT number FROM numbers(4);
INSERT INTO mv_definer SETTINGS log_comment = 'insert_into_view', async_insert = 0 VALUES (5, 'five');
INSERT INTO mv_none SETTINGS log_comment = 'insert_into_none', async_insert = 0 VALUES (6);
INSERT INTO src_dropped SETTINGS log_comment = 'dropped_target', async_insert = 0, ignore_materialized_views_with_dropped_target_table = 1 VALUES (7);
INSERT INTO src_no_push SETTINGS log_comment = 'no_push', async_insert = 0 VALUES (8); -- { serverError NOT_IMPLEMENTED }

-- Read rows, not an aggregate: an aggregate may be answered from part statistics without reading any part,
-- and then no partition is recorded.
SELECT id FROM dst_definer ORDER BY id SETTINGS log_comment = 'read_target' FORMAT Null;
SELECT id FROM mv_definer ORDER BY id SETTINGS log_comment = 'read_definer' FORMAT Null;
SELECT id FROM mv_chain ORDER BY id SETTINGS log_comment = 'read_memory' FORMAT Null;
SELECT id FROM mv_legacy ORDER BY id SETTINGS log_comment = 'read_legacy' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'caller rows';
SELECT
    log_comment,
    has(tables, currentDatabase() || '.lookup') AS reads_lookup,
    arrayExists(x -> startsWith(x, currentDatabase() || '.dst_definer'), arrayConcat(tables, partitions)) AS dst_definer,
    arrayExists(x -> startsWith(x, currentDatabase() || '.dst_legacy'), arrayConcat(tables, partitions)) AS dst_legacy,
    has(views, currentDatabase() || '.mv_chain') AS triggers_mv_chain
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
    AND log_comment IN ('insert', 'insert_into_view', 'read_target', 'read_definer', 'read_legacy')
ORDER BY log_comment;

SELECT 'child rows';
SELECT
    c.log_comment,
    replaceAll(c.query, currentDatabase() || '.', '') AS body,
    multiIf(c.user = currentUser(), 'definer', c.user = '', 'none', 'other') AS identity,
    has(c.tables, currentDatabase() || '.lookup') AS reads_lookup,
    arrayExists(x -> startsWith(x, currentDatabase() || '.dst_definer'), arrayConcat(c.tables, c.partitions)) AS dst_definer,
    has(c.views, currentDatabase() || '.mv_chain') AS triggers_mv_chain,
    c.query_id != c.initial_query_id AND p.log_comment = c.log_comment AS of_caller
FROM system.query_log AS c
LEFT JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
) AS p ON c.initial_query_id = p.query_id
WHERE c.current_database = currentDatabase() AND NOT c.is_initial_query AND c.type = 'QueryFinish'
    AND c.log_comment IN ('insert', 'insert_into_view', 'read_target', 'read_definer', 'read_legacy')
ORDER BY c.log_comment, body;

SELECT 'distinct child ids';
SELECT count() > 1, count() = uniqExact(query_id)
FROM system.query_log
WHERE current_database = currentDatabase() AND NOT is_initial_query
    AND log_comment IN ('insert', 'insert_into_view', 'insert_into_none', 'dropped_target', 'no_push', 'read_target', 'read_definer', 'read_memory', 'read_legacy');

SELECT 'user_query_log';
SELECT log_comment
FROM system.user_query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('insert', 'insert_into_view', 'read_target', 'read_definer', 'read_legacy')
ORDER BY log_comment;

SELECT 'mv read child rows';
SELECT
    log_comment,
    replaceAll(query, currentDatabase() || '.', '') AS body,
    arrayStringConcat(arrayMap(x -> replaceAll(x, currentDatabase() || '.', ''), tables), ',') AS tables_read,
    arrayStringConcat(arrayMap(x -> replaceAll(x, currentDatabase() || '.', ''), columns), ',') AS columns_read,
    arrayStringConcat(arrayMap(x -> replaceAll(x, currentDatabase() || '.', ''), partitions), ',') AS partitions_read,
    arrayStringConcat(arrayMap(x -> replaceAll(x, currentDatabase(), 'db'), databases), ',') AS databases_read
FROM system.query_log
WHERE current_database = currentDatabase() AND NOT is_initial_query AND type = 'QueryFinish'
    AND log_comment IN ('read_definer', 'read_memory')
ORDER BY log_comment;

SELECT 'insert rows tables';
SELECT
    log_comment,
    if(is_initial_query, 'caller', replaceAll(query, currentDatabase() || '.', '')) AS body,
    multiIf(user = currentUser(), 'definer', user = '', 'none', 'other') AS identity,
    arrayStringConcat(arrayMap(x -> replaceAll(x, currentDatabase() || '.', ''), tables), ',') AS tables_accessed
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('dropped_target', 'insert', 'insert_into_none')
ORDER BY log_comment, is_initial_query DESC, body;

SELECT 'no push';
SELECT countIf(is_initial_query), countIf(NOT is_initial_query)
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'no_push';
