-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database: a Replicated database runs TRUNCATE through its DDL queue, so the work is not in the query_log row of the query.
-- no-shared-merge-tree: the test is about the empty part cleanup of MergeTree.

DROP TABLE IF EXISTS t_cep;

-- old_parts_lifetime is pinned so that the empty parts dropped by TRUNCATE stay outdated during the test.
CREATE TABLE t_cep (p UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
    SETTINGS old_parts_lifetime = 480, merge_tree_clear_old_parts_interval_seconds = 100000;
-- Only the TRUNCATE's own cleanup may look at the parts it drops. SYSTEM STOP CLEANUP cannot stop a cleanup iteration
-- already in flight; the pinned interval keeps that iteration's parts pass shut.
SYSTEM STOP CLEANUP t_cep;
INSERT INTO t_cep SELECT number FROM numbers(50) SETTINGS max_partitions_per_insert_block = 0, async_insert = 0;
TRUNCATE TABLE t_cep;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_cep' AND active;
-- The TRUNCATE ran the cleanup over the empty parts it outdated.
SELECT countIf(rows = 0) > 0, countIf(removal_state = 'Cleanup thread hasn''t seen this part yet')
FROM system.parts WHERE database = currentDatabase() AND table = 't_cep' AND NOT active;

SYSTEM FLUSH LOGS query_log;
-- Dropping the 50 empty parts takes the parts lock about once per part, not once more per part for a scan of all outdated parts.
SELECT ProfileEvents['PartsLocks'] < 75 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Drop' AND query LIKE 'TRUNCATE%';

DROP TABLE t_cep;
