-- With `enable_parallel_replicas = 1` the runtime-filter pass runs while every join is still logical, so the order
-- a nested join emits is predicted from the `join_algorithm` list (`predictMergeJoinOutputOrder`). `GraceHashJoin`
-- does not support `ASOF`, so even with a spill threshold `grace_hash` leaves an `ASOF` join to the next listed
-- algorithm, and the prediction must skip it too. If it stopped at it, the nested `sorted_merge` join would be taken
-- as unordered, the join above it would get a runtime filter, and planting the filter erases its merge algorithms.
-- See PR #112973 review.

DROP TABLE IF EXISTS psg_a;
DROP TABLE IF EXISTS psg_b;
DROP TABLE IF EXISTS psg_c;

CREATE TABLE psg_a (id UInt64, ts UInt64, v UInt64) ENGINE = MergeTree ORDER BY (id, ts);
CREATE TABLE psg_b (id UInt64, ts UInt64, v UInt64) ENGINE = MergeTree ORDER BY (id, ts);
CREATE TABLE psg_c (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO psg_a SELECT number % 5000, number, number FROM numbers(20000);
INSERT INTO psg_b SELECT number % 4000, number, number FROM numbers(20000);
INSERT INTO psg_c SELECT number * 2, number FROM numbers(6000);

-- Pin the settings randomized in CI that the plan shape depends on. The spill threshold is nonzero, so
-- `grace_hash` is not skipped for lack of one.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, max_threads = 4;
SET max_bytes_before_external_join = 100000000, max_bytes_ratio_before_external_join = 0, explain_query_plan_default = 'legacy';

-- The tables are not replicated and `parallel_replicas_for_non_replicated_merge_tree = 0`, so the reads stay
-- local (and in order), but the runtime-filter pass still runs on the logical plan, as it does for parallel replicas.
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 0, parallel_replicas_plan_based = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 1;

-- Reference: without `grace_hash` in the nested list, both joins are merge joins and no runtime filter is planted.
SELECT 'without_grace_hash', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%RuntimeFilter%') = 0
FROM (EXPLAIN PIPELINE
    SELECT sum(s.v + c.v)
    FROM (SELECT a.id AS id, a.v AS v FROM psg_a AS a ASOF INNER JOIN psg_b AS b ON a.id = b.id AND a.ts >= b.ts SETTINGS join_algorithm = 'sorted_merge,hash') AS s
    ANY RIGHT JOIN psg_c AS c ON s.id = c.id
    SETTINGS join_algorithm = 'sorted_merge,hash');

-- `grace_hash` declines the nested `ASOF` join, so its list behaves as `sorted_merge,hash`.
SELECT 'grace_hash_declines_asof', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%RuntimeFilter%') = 0
FROM (EXPLAIN PIPELINE
    SELECT sum(s.v + c.v)
    FROM (SELECT a.id AS id, a.v AS v FROM psg_a AS a ASOF INNER JOIN psg_b AS b ON a.id = b.id AND a.ts >= b.ts SETTINGS join_algorithm = 'grace_hash,sorted_merge,hash') AS s
    ANY RIGHT JOIN psg_c AS c ON s.id = c.id
    SETTINGS join_algorithm = 'sorted_merge,hash');

-- The result matches a plain `hash` join.
SELECT 'grace_hash_declines_asof_result',
    (SELECT sum(s.v + c.v), count()
     FROM (SELECT a.id AS id, a.v AS v FROM psg_a AS a ASOF INNER JOIN psg_b AS b ON a.id = b.id AND a.ts >= b.ts SETTINGS join_algorithm = 'grace_hash,sorted_merge,hash') AS s
     ANY RIGHT JOIN psg_c AS c ON s.id = c.id
     SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT sum(s.v + c.v), count()
     FROM (SELECT a.id AS id, a.v AS v FROM psg_a AS a ASOF INNER JOIN psg_b AS b ON a.id = b.id AND a.ts >= b.ts SETTINGS join_algorithm = 'hash') AS s
     ANY RIGHT JOIN psg_c AS c ON s.id = c.id
     SETTINGS join_algorithm = 'hash');

DROP TABLE psg_a;
DROP TABLE psg_b;
DROP TABLE psg_c;
