-- An `Alias` over a `MergeTree` read with parallel replicas must merge the aggregated replica results
-- in the memory-efficient mode, like the table it forwards to.

DROP TABLE IF EXISTS mt_alias_pr_merge;
DROP TABLE IF EXISTS alias_pr_merge;

SET allow_experimental_alias_table_engine = 1;

CREATE TABLE mt_alias_pr_merge (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO mt_alias_pr_merge SELECT number % 10, number FROM numbers(1000);
CREATE TABLE alias_pr_merge ENGINE = Alias('mt_alias_pr_merge');

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
-- Mode 2 only collects statistics and does not use parallel replicas.
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_plan_based = 0;
SET distributed_aggregation_memory_efficient = 1;

SELECT 'MergeTree', count()
FROM viewExplain('EXPLAIN', 'actions = 1', (SELECT k, sum(v) FROM mt_alias_pr_merge GROUP BY k))
WHERE explain LIKE '%Mode: memory-efficient%';

SELECT 'Alias', count()
FROM viewExplain('EXPLAIN', 'actions = 1', (SELECT k, sum(v) FROM alias_pr_merge GROUP BY k))
WHERE explain LIKE '%Mode: memory-efficient%';

SELECT k, sum(v) FROM alias_pr_merge GROUP BY k ORDER BY k;

DROP TABLE alias_pr_merge;
DROP TABLE mt_alias_pr_merge;
