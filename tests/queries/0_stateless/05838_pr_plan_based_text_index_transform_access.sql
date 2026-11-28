-- A text index transform runs with the rights of whoever declared the index, so a user who may not call its function
-- can still search through the index. Under plan-based parallel replicas (`parallel_replicas_plan_based`) a replica
-- rebuilds every function of the shipped plan with the rights of the reader, so such a read stays on the initiator.

DROP TABLE IF EXISTS tab_post;
DROP TABLE IF EXISTS tab_pre;
DROP TABLE IF EXISTS tab_outer;

SET allow_introspection_functions = 1;

CREATE TABLE tab_post (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = demangle(val)))
ENGINE = MergeTree ORDER BY id;
CREATE TABLE tab_pre (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)))
ENGINE = MergeTree ORDER BY id;
CREATE TABLE tab_outer (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_post VALUES (1, 'hello world'), (2, 'bye');
INSERT INTO tab_pre VALUES (1, 'hello world'), (2, 'bye');
INSERT INTO tab_outer SELECT number FROM numbers(10);

SET enable_parallel_replicas = 1;
SET parallel_replicas_plan_based = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
-- Every replica is a remote one, and the filter is evaluated on the rows instead of being answered by the index alone.
SET parallel_replicas_local_plan = 0;
SET query_plan_direct_read_from_text_index = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;

SELECT '--- granted: the read is shipped';
SELECT count() FROM tab_post WHERE hasAllTokens(val, ['hello']);
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM tab_post WHERE hasAllTokens(val, ['hello']));

SET allow_introspection_functions = 0;

SELECT '--- denied: the read stays local';
SELECT count() FROM tab_post WHERE hasAllTokens(val, ['hello']);
SELECT count() FROM tab_post WHERE hasToken(val, 'hello');
SELECT count() FROM tab_pre WHERE hasAllTokens(val, ['hello']);
SELECT count() FROM tab_outer WHERE id IN (SELECT id FROM tab_post WHERE hasAllTokens(val, ['hello']));
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM tab_post WHERE hasAllTokens(val, ['hello']));

SELECT '--- the check adds nothing to the privileges the query reports';
SELECT count() FROM tab_post WHERE hasAllTokens(val, ['hello']) SETTINGS log_comment = '05838_pr_plan_based_text_index_transform_access_denied';
SYSTEM FLUSH LOGS query_log;
SELECT empty(missing_privileges) FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05838_pr_plan_based_text_index_transform_access_denied'
    AND type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday();

DROP TABLE tab_post;
DROP TABLE tab_pre;
DROP TABLE tab_outer;
