-- An imprecise estimate does not authorize a broadcast on its own: the row bound decides. `t_part_stats` has statistics
-- in one of its two parts only, so the estimate of `a = 5` (2,000 rows) extends them to all rows and is imprecise; its
-- bound is the 20,000 rows of the table. With a broadcast budget of 50,000 bytes the estimate would fit and the bound
-- does not, so the join shuffles instead. Once both parts have statistics, the estimate is precise and broadcasts.
SET enable_analyzer = 1;
SET use_statistics = 1;
SET allow_statistic_optimize = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 4;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0;
SET enable_join_runtime_filters = 0;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_part_stats;
DROP TABLE IF EXISTS t_fact;
CREATE TABLE t_part_stats (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'uniq';
-- A merge would build the statistics the second part lacks.
SYSTEM STOP MERGES t_part_stats;
INSERT INTO t_part_stats SELECT number % 10 FROM numbers(10000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO t_part_stats SELECT 100 + number FROM numbers(10000) SETTINGS materialize_statistics_on_insert = 0;
CREATE TABLE t_fact (a UInt64, v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = 'uniq';
INSERT INTO t_fact SELECT number % 1000, number FROM numbers(2000000) SETTINGS materialize_statistics_on_insert = 1;

SELECT '-- statistics in one part: the join does not broadcast';
SELECT extract(explain, '(Broadcast|Shuffle|Local) HashJoin') FROM (
    EXPLAIN SELECT count() FROM t_fact AS f JOIN (SELECT a FROM t_part_stats WHERE a = 5) AS s ON f.a = s.a
    SETTINGS distributed_plan_max_bytes_to_broadcast = 50000)
WHERE explain LIKE '%JoinLogical%';

SYSTEM START MERGES t_part_stats;
ALTER TABLE t_part_stats MATERIALIZE STATISTICS ALL;

SELECT '-- statistics in both parts: the join broadcasts';
SELECT extract(explain, '(Broadcast|Shuffle|Local) HashJoin') FROM (
    EXPLAIN SELECT count() FROM t_fact AS f JOIN (SELECT a FROM t_part_stats WHERE a = 5) AS s ON f.a = s.a
    SETTINGS distributed_plan_max_bytes_to_broadcast = 50000)
WHERE explain LIKE '%JoinLogical%';

-- A join of keys without NDVs takes the keys as a foreign key meeting a primary key: an assumption, so its estimate is
-- imprecise too. Here every key is 7 and the 1,000-row estimate of `t_na JOIN t_nb` is in fact 1,000,000 rows: the
-- bound decides, and the join is not replicated.
DROP TABLE IF EXISTS t_na;
DROP TABLE IF EXISTS t_nb;
CREATE TABLE t_na (k UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'minmax';
CREATE TABLE t_nb (k UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'minmax';
INSERT INTO t_na SELECT 7 FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO t_nb SELECT 7 FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;

SELECT '-- a join of keys without NDVs: the outer join does not replicate it (expect 0)';
SELECT extract(explain, '(Broadcast|Replicated) ') != '' FROM (
    EXPLAIN SELECT count() FROM t_fact AS f JOIN (SELECT a.k AS k FROM t_na AS a JOIN t_nb AS b ON a.k = b.k) AS s ON f.a = s.k
    SETTINGS distributed_plan_max_bytes_to_broadcast = 100000, query_plan_optimize_join_order_randomize = 0,
        query_plan_optimize_join_order_limit = 0)
WHERE explain LIKE '%JoinLogical%'
LIMIT 1;

DROP TABLE t_na;
DROP TABLE t_nb;
DROP TABLE t_part_stats;
DROP TABLE t_fact;
