-- `x = x` on a column where `=` is reflexive and never NULL passes every row, so it must not shrink the row estimate.
-- A correlated subquery keeps one such conjunct per correlated key after decorrelation.
-- https://github.com/ClickHouse/ClickHouse/issues/122677
-- Random settings limits: query_plan_optimize_join_order_limit=(1, None); use_statistics=(1, 1); materialize_statistics_on_insert=(1, 1); query_plan_optimize_join_order_randomize=(0, 0)
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1 (k Int64, c1 Int64, c2 Int64, c3 Int64, c4 Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t2 (k Int64, c1 Int64, c2 Int64, c3 Int64, c4 Int64, s LowCardinality(String), fs FixedString(2), dec Decimal(18, 2), e Enum8('a' = 1, 'b' = 2),
    d Date, dt DateTime64(3), u UUID, ip4 IPv4, ip6 IPv6, f Float64, n Nullable(Int64), t Tuple(Int64), ln LowCardinality(Nullable(String)))
ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic';
CREATE TABLE t3 (k Int64, c1 Int64, c2 Int64, c3 Int64, c4 Int64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t1 SELECT number, number % 10, number % 7, number % 3, number % 2 FROM numbers(10000);
INSERT INTO t2 SELECT number, number % 10, number % 7, number % 3, number % 2, toString(number), toString(number % 100), number, if(number % 2, 'a', 'b'),
    toDate(number), toDateTime64(number, 3), generateUUIDv4(), toIPv4(number), toIPv6(toIPv4(number)), number, number, tuple(number), toString(number) FROM numbers(10000);
INSERT INTO t3 SELECT number, number % 10, number % 7, number % 3, number % 2 FROM numbers(10000);

SELECT '-- the inner table of the correlated subquery is estimated at its full size, with and without statistics';
SELECT count() FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM t1 AS a WHERE EXISTS (SELECT 1 FROM t2 AS b WHERE a.k = b.k AND a.c1 = b.c1 AND a.c2 = b.c2 AND a.c3 = b.c3 AND a.c4 = b.c4)
    SETTINGS collect_hash_table_stats_during_joins = 0)
WHERE match(explain, 't2\\[10000\\]');
SELECT count() FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM t1 AS a WHERE EXISTS (SELECT 1 FROM t3 AS b WHERE a.k = b.k AND a.c1 = b.c1 AND a.c2 = b.c2 AND a.c3 = b.c3 AND a.c4 = b.c4)
    SETTINGS collect_hash_table_stats_during_joins = 0)
WHERE match(explain, 't3\\[no_stats~10000\\]');

SELECT '-- next to another condition, the estimate is the one of that condition alone';
WITH
    (SELECT extract(explain, 't2\\[(?:no_stats~)?(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM t1 AS a WHERE EXISTS (SELECT 1 FROM t2 AS b WHERE a.k = b.k AND b.c2 < 3)
        SETTINGS collect_hash_table_stats_during_joins = 0) WHERE explain LIKE '%t2[%') AS with_equality,
    (SELECT extract(explain, 'b\\[(?:no_stats~)?(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM t1 AS a JOIN t2 AS b ON a.k = b.k WHERE b.c2 < 3) WHERE explain LIKE '%⋈%') AS alone
SELECT with_equality != '' AND with_equality = alone;

SELECT '-- full size for reflexive types, the estimate of an unknown condition for Float64 (NaN), Nullable (NULL) and Tuple';
SELECT 'reflexive', count() FROM (EXPLAIN actions = 1, compact = 1, pretty = 1 SELECT count() FROM t1 AS a JOIN t2 AS b ON a.k = b.k
    WHERE b.c1 = b.c1 AND b.s = b.s AND b.fs = b.fs AND b.dec = b.dec AND b.e = b.e AND b.d = b.d AND b.dt = b.dt AND b.u = b.u AND b.ip4 = b.ip4 AND b.ip6 = b.ip6)
WHERE match(explain, 'b\\[(no_stats~)?10000\\]');
WITH
    (SELECT extract(explain, 'b\\[(?:no_stats~)?(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM t1 AS a JOIN t2 AS b ON a.k = b.k WHERE b.c1 = b.c2) WHERE explain LIKE '%⋈%') AS unknown,
    (SELECT extract(explain, 'b\\[(?:no_stats~)?(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM t1 AS a JOIN t2 AS b ON a.k = b.k WHERE b.f = b.f) WHERE explain LIKE '%⋈%') AS f,
    (SELECT extract(explain, 'b\\[(?:no_stats~)?(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM t1 AS a JOIN t2 AS b ON a.k = b.k WHERE b.n = b.n) WHERE explain LIKE '%⋈%') AS n,
    (SELECT extract(explain, 'b\\[(?:no_stats~)?(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM t1 AS a JOIN t2 AS b ON a.k = b.k WHERE b.ln = b.ln) WHERE explain LIKE '%⋈%') AS ln,
    (SELECT extract(explain, 'b\\[(?:no_stats~)?(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM t1 AS a JOIN t2 AS b ON a.k = b.k WHERE b.t = b.t) WHERE explain LIKE '%⋈%') AS t
SELECT unknown != '' AND unknown != '10000', f = unknown, n = unknown, ln = unknown, t = unknown;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
