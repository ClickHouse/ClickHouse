-- Tags: no-parallel, no-fasttest
-- Tag no-parallel, no-fasttest: uses a failpoint

-- Read in order through `LEFT JOIN` with parallel replicas: every replica must choose the same
-- reading mode, and the merged result must stay sorted.

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS rj;

CREATE TABLE l (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY (k, v) SETTINGS index_granularity = 128;
SYSTEM STOP MERGES l;
-- Every part covers the whole key range, so the replicas have to merge their streams.
INSERT INTO l SELECT (number * 7) % 75000, number FROM numbers(300000);
INSERT INTO l SELECT (number * 11) % 75000, number + 300000 FROM numbers(300000);
INSERT INTO l SELECT (number * 13) % 75000, number + 600000 FROM numbers(300000);

CREATE TABLE r (k UInt32, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO r SELECT number * 2, toString(number) FROM numbers(2500);

CREATE TABLE rj (k UInt32, s String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO rj SELECT number * 2, toString(number) FROM numbers(2500);

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1, optimize_aggregation_in_order = 1, optimize_distinct_in_order = 1;
SET query_plan_read_in_order_through_join = 1;
SET join_algorithm = 'hash', query_plan_join_swap_table = false, query_plan_optimize_join_order_randomize = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0;

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1;

SELECT 'left join, order by';
SELECT arraySort(groupArray(trim(explain))) FROM (
    EXPLAIN actions = 1
    SELECT l.k, l.v, r.s FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.k, l.v LIMIT 5
) WHERE explain LIKE '%ReadType%';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT l.k, l.v, r.s FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.k, l.v LIMIT 5;
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT l.k, l.v, r.s FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.k DESC, l.v DESC LIMIT 5;

SELECT 'left join, group by';
SELECT arraySort(groupArray(trim(explain))) FROM (
    EXPLAIN actions = 1
    SELECT l.k, count(), any(r.s) FROM l LEFT JOIN r ON l.k = r.k GROUP BY l.k ORDER BY l.k LIMIT 5
) WHERE explain LIKE '%ReadType%';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT l.k, count(), any(r.s) FROM l LEFT JOIN r ON l.k = r.k GROUP BY l.k ORDER BY l.k LIMIT 5;

SELECT 'left join, distinct';
SELECT arraySort(groupArray(trim(explain))) FROM (
    EXPLAIN actions = 1
    SELECT DISTINCT l.k, r.s FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.k LIMIT 5
) WHERE explain LIKE '%ReadType%';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT DISTINCT l.k, r.s FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.k LIMIT 5;

-- A `Join`-engine table is joined by a filled join, which keeps the left order without a pin.
SELECT 'left join engine, order by';
SELECT arraySort(groupArray(trim(explain))) FROM (
    EXPLAIN actions = 1
    SELECT l.k, l.v, rj.s FROM l LEFT ANY JOIN rj ON l.k = rj.k ORDER BY l.k, l.v LIMIT 5
) WHERE explain LIKE '%ReadType%';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT l.k, l.v, rj.s FROM l LEFT ANY JOIN rj ON l.k = rj.k ORDER BY l.k, l.v LIMIT 5;

-- The virtual row bounds differ between replicas with different parts, so an inner join is read
-- in order only when forced.
SELECT 'inner join, order by';
SELECT arraySort(groupArray(trim(explain))) FROM (
    EXPLAIN actions = 1
    SELECT l.k, l.v, r.s FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k, l.v LIMIT 5
) WHERE explain LIKE '%ReadType%';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT l.k, l.v, r.s FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k, l.v LIMIT 5;

SELECT 'inner join, order by, force';
SELECT arraySort(groupArray(trim(explain))) FROM (
    EXPLAIN actions = 1
    SELECT l.k, l.v, r.s FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k, l.v LIMIT 5
    SETTINGS query_plan_read_in_order_through_join = 'force'
) WHERE explain LIKE '%ReadType%';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT l.k, l.v, r.s FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k, l.v LIMIT 5
SETTINGS query_plan_read_in_order_through_join = 'force';

-- The whole ordered result is the same with and without parallel replicas. A single thread leaves
-- the remote replicas time to take part in reading.
SELECT 'checksums';
SET max_threads = 1;
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT cityHash64(groupArray((k, v, s))), count() FROM (SELECT l.k, l.v, r.s FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.k, l.v);
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT cityHash64(groupArray((k, c))), count() FROM (SELECT l.k, count() AS c FROM l LEFT JOIN r ON l.k = r.k GROUP BY l.k ORDER BY l.k);
SYSTEM DISABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SET enable_parallel_replicas = 0;
SELECT cityHash64(groupArray((k, v, s))), count() FROM (SELECT l.k, l.v, r.s FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.k, l.v);
SELECT cityHash64(groupArray((k, c))), count() FROM (SELECT l.k, count() AS c FROM l LEFT JOIN r ON l.k = r.k GROUP BY l.k ORDER BY l.k);

DROP TABLE l;
DROP TABLE r;
DROP TABLE rj;
