-- `SELECT DISTINCT ... ORDER BY ... LIMIT N`: the preliminary `DISTINCT` keeps only the `N` best keys of each stream.

SET enable_analyzer = 1;
SET max_threads = 4, max_block_size = 1000;
-- The queries below rely on the optimization being applied.
SET enable_distinct_top_k_optimization = 1, query_plan_max_limit_for_top_k_optimization = 1000, use_top_k_dynamic_filtering = 1;
-- The plan checks below expect a local read: with parallel replicas the plan is nested and the boundary does not reach the reads.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_distinct_top_k;
CREATE TABLE t_distinct_top_k (id UInt64, ts UInt64, value String, n Nullable(UInt32))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000;

-- `ts` is unique, so the best row of each key is unique too.
INSERT INTO t_distinct_top_k SELECT number, (number * 7919) % 200000, toString(number % 5000), if(number % 7 = 0, NULL, number % 3000) FROM numbers(200000);

SELECT 'plan';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT DISTINCT value FROM t_distinct_top_k ORDER BY ts LIMIT 10 OFFSET 5) WHERE explain LIKE '%Top-K%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT DISTINCT value FROM t_distinct_top_k ORDER BY ts LIMIT 10 WITH TIES) WHERE explain LIKE '%Top-K%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT DISTINCT value FROM t_distinct_top_k ORDER BY ts LIMIT 2000) WHERE explain LIKE '%Top-K%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT DISTINCT value FROM t_distinct_top_k ORDER BY ts LIMIT 10 SETTINGS enable_distinct_top_k_optimization = 0) WHERE explain LIKE '%Top-K%';

-- The boundary on `ts` filters the rows of the read.
SELECT count() FROM (EXPLAIN actions = 1 SELECT DISTINCT value FROM t_distinct_top_k ORDER BY ts LIMIT 10) WHERE explain LIKE '%Top-K dynamic filtering: 1%';
-- A computed sort column is computed below the preliminary `DISTINCT` too, unless it is not deterministic.
SELECT count() FROM (EXPLAIN actions = 1 SELECT DISTINCT value FROM t_distinct_top_k ORDER BY ts % 1000, ts LIMIT 10) WHERE explain LIKE '%Top-K sort description: ts MOD 1000 ASC, ts ASC%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT DISTINCT value FROM t_distinct_top_k ORDER BY rand() LIMIT 10) WHERE explain LIKE '%Top-K%';

SELECT 'order by the key';
SELECT DISTINCT value FROM t_distinct_top_k ORDER BY value LIMIT 5;
SELECT DISTINCT value FROM t_distinct_top_k ORDER BY value DESC LIMIT 5 OFFSET 3;
SELECT DISTINCT n FROM t_distinct_top_k ORDER BY n DESC NULLS FIRST LIMIT 4;
SELECT DISTINCT n FROM t_distinct_top_k ORDER BY n NULLS LAST LIMIT 3 OFFSET 2;
SELECT DISTINCT toLowCardinality(value) AS v FROM t_distinct_top_k ORDER BY v DESC LIMIT 3;

SELECT 'order by another column: the best row of each key';
SELECT DISTINCT value FROM t_distinct_top_k ORDER BY ts LIMIT 5;
SELECT DISTINCT value, ts FROM t_distinct_top_k ORDER BY ts DESC LIMIT 5 OFFSET 1;
SELECT
    (SELECT groupArray(value) FROM (SELECT DISTINCT value FROM t_distinct_top_k ORDER BY ts LIMIT 100))
    = (SELECT groupArray(value) FROM (SELECT value FROM t_distinct_top_k GROUP BY value ORDER BY min(ts) LIMIT 100));
SELECT
    (SELECT groupArray(value) FROM (SELECT DISTINCT value FROM t_distinct_top_k ORDER BY ts DESC LIMIT 300 OFFSET 7))
    = (SELECT groupArray(value) FROM (SELECT value FROM t_distinct_top_k GROUP BY value ORDER BY max(ts) DESC LIMIT 300 OFFSET 7));
-- A computed sort column.
SELECT
    (SELECT groupArray(value) FROM (SELECT DISTINCT value FROM t_distinct_top_k ORDER BY ts % 1000, ts LIMIT 50))
    = (SELECT groupArray(value) FROM (SELECT value FROM t_distinct_top_k GROUP BY value ORDER BY min((ts % 1000, ts)) LIMIT 50));
-- Two `ORDER BY` columns, two `DISTINCT` keys.
SELECT
    (SELECT groupArray((value, r)) FROM (SELECT DISTINCT value, id % 3 AS r FROM t_distinct_top_k ORDER BY n DESC NULLS LAST, ts LIMIT 50))
    = (SELECT groupArray((value, r)) FROM (
        SELECT value, r FROM (SELECT value, id % 3 AS r, argMin((-ifNull(n, -1), ts), (-ifNull(n, -1), ts)) AS best FROM t_distinct_top_k GROUP BY value, r)
        ORDER BY best LIMIT 50));

SELECT 'read in order: the preliminary DISTINCT stops after the first keys of each stream';
DROP TABLE IF EXISTS t_distinct_top_k_in_order;
CREATE TABLE t_distinct_top_k_in_order (id UInt64, ts UInt64, value String) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1000;
INSERT INTO t_distinct_top_k_in_order SELECT id, ts, value FROM t_distinct_top_k;
SELECT
    (SELECT groupArray(value) FROM (SELECT DISTINCT value FROM t_distinct_top_k_in_order ORDER BY ts LIMIT 100 SETTINGS optimize_read_in_order = 1))
    = (SELECT groupArray(value) FROM (SELECT value FROM t_distinct_top_k GROUP BY value ORDER BY min(ts) LIMIT 100));
SELECT
    (SELECT groupArray(value) FROM (SELECT DISTINCT value FROM t_distinct_top_k_in_order ORDER BY ts DESC LIMIT 100 OFFSET 10 SETTINGS optimize_read_in_order = 1))
    = (SELECT groupArray(value) FROM (SELECT value FROM t_distinct_top_k GROUP BY value ORDER BY max(ts) DESC LIMIT 100 OFFSET 10));
DROP TABLE t_distinct_top_k_in_order;

SELECT 'memory is bounded by the limit';
SELECT DISTINCT number AS x FROM numbers_mt(10000000) ORDER BY x DESC LIMIT 3 SETTINGS max_memory_usage = '100Mi';
SELECT DISTINCT x FROM (SELECT intDiv(number, 2) AS x, number % 1000003 AS o FROM numbers_mt(10000000)) ORDER BY o, x LIMIT 3 SETTINGS max_memory_usage = '100Mi';

DROP TABLE t_distinct_top_k;
