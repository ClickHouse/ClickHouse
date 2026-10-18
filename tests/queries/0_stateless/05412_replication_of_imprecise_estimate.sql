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

-- A part without statistics that is small next to the estimate cannot make it far off: the 100 rows of the second
-- part could add at most 100 rows to the estimate of 1,000, so the estimate stays precise and the join broadcasts.
DROP TABLE IF EXISTS t_small_gap;
CREATE TABLE t_small_gap (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'uniq';
SYSTEM STOP MERGES t_small_gap;
INSERT INTO t_small_gap SELECT number % 10 FROM numbers(10000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO t_small_gap SELECT 100 + number FROM numbers(100) SETTINGS materialize_statistics_on_insert = 0;

SELECT '-- statistics on all but 100 rows: the join broadcasts';
SELECT extract(explain, '(Broadcast|Shuffle|Local) HashJoin') FROM (
    EXPLAIN SELECT count() FROM t_fact AS f JOIN (SELECT a FROM t_small_gap WHERE a = 5) AS s ON f.a = s.a
    SETTINGS distributed_plan_max_bytes_to_broadcast = 50000)
WHERE explain LIKE '%JoinLogical%';
DROP TABLE t_small_gap;

-- Few rows without statistics can still add many distinct values: 10,000 rows of one value have statistics, 100 rows
-- of 100 new values do not. `DISTINCT k` has 101 rows, while the statistics see 1, so its estimate is imprecise and
-- the bound of 10,100 rows does not fit the budget.
DROP TABLE IF EXISTS t_new_values;
CREATE TABLE t_new_values (k UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'uniq';
SYSTEM STOP MERGES t_new_values;
INSERT INTO t_new_values SELECT 7 FROM numbers(10000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO t_new_values SELECT 1000 + number FROM numbers(100) SETTINGS materialize_statistics_on_insert = 0;

SELECT '-- 100 new values without statistics: the join of their DISTINCT does not broadcast';
SELECT extract(explain, '(Broadcast|Shuffle|Local) HashJoin') FROM (
    EXPLAIN SELECT count() FROM t_fact AS f JOIN (SELECT DISTINCT k FROM t_new_values) AS s ON f.a = s.k
    SETTINGS distributed_plan_max_bytes_to_broadcast = 50000)
WHERE explain LIKE '%JoinLogical%';

-- The same new values reach the estimates above the read: an `INTERSECT` that takes the NDV of this input, and a
-- filter above a `LIMIT` that estimates `k != 7` from the NDV of 1.
DROP TABLE IF EXISTS t_keys;
CREATE TABLE t_keys (k UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'uniq';
INSERT INTO t_keys SELECT number FROM numbers(10000) SETTINGS materialize_statistics_on_insert = 1;
SELECT '-- an INTERSECT with the new values: the join does not broadcast';
SELECT extract(explain, '(Broadcast|Shuffle|Local) HashJoin') FROM (
    EXPLAIN SELECT count() FROM t_fact AS f JOIN (SELECT k FROM t_keys INTERSECT DISTINCT SELECT k FROM t_new_values) AS s ON f.a = s.k
    SETTINGS distributed_plan_max_bytes_to_broadcast = 50000)
WHERE explain LIKE '%JoinLogical%';
SELECT '-- a filter on the new values above a LIMIT: the join does not broadcast';
SELECT extract(explain, '(Broadcast|Shuffle|Local) HashJoin') FROM (
    EXPLAIN SELECT count() FROM t_fact AS f JOIN (SELECT k FROM (SELECT k FROM t_new_values ORDER BY k DESC LIMIT 10050) WHERE k != 7) AS s ON f.a = s.k
    SETTINGS distributed_plan_max_bytes_to_broadcast = 50000)
WHERE explain LIKE '%JoinLogical%';
DROP TABLE t_keys;
DROP TABLE t_new_values;

-- A function can fold the known values into fewer: 365 dates of one year have statistics, 100 dates of 100 later years
-- do not. The 100 rows cannot double the 365 dates, but they can more than double the 1 year that `toYear` leaves. The
-- years are 2 bytes wide, so the budget is smaller than the bound of 10,100 rows.
DROP TABLE IF EXISTS t_new_years;
CREATE TABLE t_new_years (d Date) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq';
SYSTEM STOP MERGES t_new_years;
INSERT INTO t_new_years SELECT toDate('2000-01-01') + number % 365 FROM numbers(10000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO t_new_years SELECT toDate('2001-01-01') + toIntervalYear(number) FROM numbers(100) SETTINGS materialize_statistics_on_insert = 0;
SELECT '-- the years of 100 new dates without statistics: the join of their DISTINCT does not broadcast';
SELECT extract(explain, '(Broadcast|Shuffle|Local) HashJoin') FROM (
    EXPLAIN SELECT count() FROM t_fact AS f JOIN (SELECT DISTINCT toYear(d) AS y FROM t_new_years) AS s ON f.a = s.y
    SETTINGS distributed_plan_max_bytes_to_broadcast = 10000)
WHERE explain LIKE '%JoinLogical%';
DROP TABLE t_new_years;

-- A comparison of two columns above a `LIMIT` (which keeps the filter out of the read): `k` has statistics on every
-- part, `b` not on the 100 rows of the second partition, which hold new values. One NDV that these rows could double
-- makes the comparison a default, in either order of the columns; with the NDV of `k` alone it would be 0 rows.
DROP TABLE IF EXISTS t_few_matches;
CREATE TABLE t_few_matches (p UInt8, k UInt64 STATISTICS(uniq), b UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
INSERT INTO t_few_matches SELECT 1, 7, 7 FROM numbers(10000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO t_few_matches SELECT 2, 7, 100000 + number FROM numbers(100) SETTINGS materialize_statistics_on_insert = 1;
ALTER TABLE t_few_matches ADD STATISTICS b TYPE uniq;
ALTER TABLE t_few_matches MATERIALIZE STATISTICS b IN PARTITION 1;
SELECT '-- a comparison of two columns, one of them with rows without statistics: the join does not broadcast';
SELECT extract(explain, '(Broadcast|Shuffle|Local) HashJoin') FROM (
    EXPLAIN SELECT count() FROM t_fact AS f JOIN (SELECT k FROM (SELECT k, b FROM t_few_matches ORDER BY k DESC LIMIT 10100) WHERE k != b) AS s ON f.a = s.k
    SETTINGS distributed_plan_max_bytes_to_broadcast = 50000)
WHERE explain LIKE '%JoinLogical%';
SELECT extract(explain, '(Broadcast|Shuffle|Local) HashJoin') FROM (
    EXPLAIN SELECT count() FROM t_fact AS f JOIN (SELECT k FROM (SELECT k, b FROM t_few_matches ORDER BY k DESC LIMIT 10100) WHERE b != k) AS s ON f.a = s.k
    SETTINGS distributed_plan_max_bytes_to_broadcast = 50000)
WHERE explain LIKE '%JoinLogical%';
DROP TABLE t_few_matches;

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
