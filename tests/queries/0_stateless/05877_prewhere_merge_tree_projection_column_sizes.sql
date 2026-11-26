-- Tags: no-parallel-replicas
-- Reads through `mergeTreeProjection` order PREWHERE by the sizes of the projection's table.

SET optimize_move_to_prewhere = 1, move_all_conditions_to_prewhere = 1, enable_parallel_replicas = 0;

CREATE TABLE t_proj_prewhere (k UInt64, a UInt64, s String, PROJECTION p (SELECT k, a, s ORDER BY a))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_proj_prewhere SELECT number, number % 10, toString(number) FROM numbers(1000);

SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT sum(length(s)) FROM mergeTreeProjection(currentDatabase(), t_proj_prewhere, p) WHERE a = 1 AND k > 5)
WHERE explain ILIKE '%Prewhere filter column%';

SELECT sum(length(s)) FROM mergeTreeProjection(currentDatabase(), t_proj_prewhere, p) WHERE a = 1 AND k > 5;

DROP TABLE t_proj_prewhere;
