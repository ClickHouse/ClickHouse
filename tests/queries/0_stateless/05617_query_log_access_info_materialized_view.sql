-- A materialized view reads its target, and on INSERT writes it, under the view's own security context: DEFINER, NONE,
-- or the legacy empty one that skips access checks. What that context touches is attributed to the caller's query_log
-- row, which is marked as hidden, and the caller's own system.user_query_log omits that row.
-- Relies on query_log.enable_query_access_info_attribution = 1 from tests/config.

SET enable_parallel_replicas = 0;
SET log_queries = 1;

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_definer (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_legacy (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_definer TO dst_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id FROM src;
CREATE MATERIALIZED VIEW mv_legacy TO dst_legacy AS SELECT id FROM src;

INSERT INTO src SETTINGS log_comment = 'insert' SELECT number FROM numbers(2);

-- Read rows, not an aggregate: an aggregate may be answered from part statistics without reading any part,
-- and then no partition is recorded.
SELECT id FROM dst_definer ORDER BY id SETTINGS log_comment = 'read_target' FORMAT Null;
SELECT id FROM mv_definer ORDER BY id SETTINGS log_comment = 'read_definer' FORMAT Null;
SELECT id FROM mv_legacy ORDER BY id SETTINGS log_comment = 'read_legacy' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- A read through the view records the target's partitions, an insert records the target table.
SELECT
    log_comment,
    arrayExists(x -> startsWith(x, currentDatabase() || '.dst_definer'), arrayConcat(tables, partitions)) AS definer_target_attributed,
    arrayExists(x -> startsWith(x, currentDatabase() || '.dst_legacy'), arrayConcat(tables, partitions)) AS legacy_target_attributed,
    query_access_info_is_hidden AS hidden
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('insert', 'read_target', 'read_definer', 'read_legacy')
ORDER BY log_comment;

-- The caller's own log keeps only the rows that are not marked.
SELECT log_comment
FROM system.user_query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('insert', 'read_target', 'read_definer', 'read_legacy')
ORDER BY log_comment;
