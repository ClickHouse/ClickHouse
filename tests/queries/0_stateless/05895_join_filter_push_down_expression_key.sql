-- A `WHERE` predicate on a `JOIN` key is also pushed down to the other side of the `JOIN`, with that side's
-- key put in its place. The key can be an expression, such as `toDate(l.ts)`: the filter does not read it
-- as a column but computes it again, so it is found by what it computes rather than by its name.
-- Every result below is the one with `query_plan_filter_push_down = 0`.

SET query_plan_filter_push_down = 1;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;
DROP TABLE IF EXISTS t_l_mt;
DROP TABLE IF EXISTS t_r_mt;

CREATE TABLE t_l (ts DateTime('UTC'), k UInt64) ENGINE = Memory;
CREATE TABLE t_r (ts DateTime('UTC'), k UInt64) ENGINE = Memory;
INSERT INTO t_l SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + number * 43200, number FROM numbers(12);
INSERT INTO t_r SELECT toDateTime('2020-01-01 06:00:00', 'UTC') + number * 43200, number FROM numbers(5, 12);

SELECT 'Function of a column: the result, then the number of tables the predicate filters';
SELECT count(), sum(l.k), sum(r.k) FROM t_l AS l INNER JOIN t_r AS r ON toDate(l.ts) = toDate(r.ts) WHERE toDate(l.ts) >= '2020-01-05';
SELECT countIf(explain LIKE '%ilter column:%toDate(ts) >= %') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_l AS l INNER JOIN t_r AS r ON toDate(l.ts) = toDate(r.ts) WHERE toDate(l.ts) >= '2020-01-05'
);
SELECT count(), sum(l.k), sum(r.k) FROM t_l AS l LEFT JOIN t_r AS r ON toDate(l.ts) = toDate(r.ts) WHERE toDate(l.ts) >= '2020-01-05';
SELECT countIf(explain LIKE '%ilter column:%toDate(ts) >= %') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_l AS l LEFT JOIN t_r AS r ON toDate(l.ts) = toDate(r.ts) WHERE toDate(l.ts) >= '2020-01-05'
);
SELECT count(), sum(l.k), sum(r.k) FROM t_l AS l RIGHT JOIN t_r AS r ON toDate(l.ts) = toDate(r.ts) WHERE toDate(r.ts) <= '2020-01-04';
SELECT countIf(explain LIKE '%ilter column:%toDate(ts) <= %') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_l AS l RIGHT JOIN t_r AS r ON toDate(l.ts) = toDate(r.ts) WHERE toDate(r.ts) <= '2020-01-04'
);

SELECT 'The other side gets its own key, which is another expression or a column';
SELECT count(), sum(l.k), sum(r.k) FROM t_l AS l INNER JOIN t_r AS r ON l.k + 1 = r.k * 2 WHERE r.k * 2 > 11;
SELECT countIf(explain LIKE '%ilter column:%k * 2 > 11%'), countIf(explain LIKE '%ilter column:%k + 1 > 11%') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_l AS l INNER JOIN t_r AS r ON l.k + 1 = r.k * 2 WHERE r.k * 2 > 11
);
SELECT count(), sum(l.k), sum(r.k) FROM t_l AS l INNER JOIN t_r AS r ON l.k + 1 = r.k WHERE l.k + 1 < 8;
SELECT countIf(explain LIKE '%ilter column:%k + 1 < 8%'), countIf(explain LIKE '%ilter column:%k < 8%') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_l AS l INNER JOIN t_r AS r ON l.k + 1 = r.k WHERE l.k + 1 < 8
);

SELECT 'The key replaces the whole expression, including when the column inside it is a key too';
SELECT count(), sum(l.k), sum(r.k) FROM t_l AS l INNER JOIN t_r AS r ON l.k + 1 = r.k + 1 WHERE l.k + 1 < 8;
SELECT countIf(explain LIKE '%ilter column:%k + 1 < 8%') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_l AS l INNER JOIN t_r AS r ON l.k + 1 = r.k + 1 WHERE l.k + 1 < 8
);
SELECT count(), sum(l.k), sum(r.k) FROM t_l AS l INNER JOIN t_r AS r ON l.k = r.k AND l.k + 1 = r.k + 1 WHERE l.k + 1 < 8;
SELECT countIf(explain LIKE '%ilter column:%k + 1 < 8%') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_l AS l INNER JOIN t_r AS r ON l.k = r.k AND l.k + 1 = r.k + 1 WHERE l.k + 1 < 8
);

SELECT 'A predicate that also reads a column that is not a key stays on its side';
SELECT count(), sum(l.k), sum(r.k) FROM t_l AS l INNER JOIN t_r AS r ON l.k + 1 = r.k + 1 WHERE l.k + 1 < 8 OR l.k = 9;
SELECT countIf(explain LIKE '%ilter column:%k + 1 < 8%') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_l AS l INNER JOIN t_r AS r ON l.k + 1 = r.k + 1 WHERE l.k + 1 < 8 OR l.k = 9
);

SELECT 'A key that is not deterministic is not the same value in the predicate';
SELECT count(), sum(l.k), sum(r.k) FROM t_l AS l INNER JOIN t_r AS r ON l.k + rand() % 1 = r.k WHERE l.k + rand() % 1 < 8;
SELECT countIf(explain LIKE '%ilter column:%rand()%') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_l AS l INNER JOIN t_r AS r ON l.k + rand() % 1 = r.k WHERE l.k + rand() % 1 < 8
);

SELECT 'The side that join_use_nulls makes Nullable';
SET join_use_nulls = 1;
SELECT count(), sum(l.k), sum(r.k) FROM t_l AS l LEFT JOIN t_r AS r ON toDate(l.ts) = toDate(r.ts) WHERE toDate(l.ts) >= '2020-01-05';
SELECT countIf(explain LIKE '%ilter column:%toDate(ts) >= %') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_l AS l LEFT JOIN t_r AS r ON toDate(l.ts) = toDate(r.ts) WHERE toDate(l.ts) >= '2020-01-05'
);
SELECT count(), sum(l.k), sum(r.k) FROM t_l AS l LEFT JOIN t_r AS r ON l.k + 1 = r.k + 1 WHERE r.k + 1 IS NULL;
SET join_use_nulls = 0;

SELECT 'The primary key of both tables is used';
CREATE TABLE t_l_mt (ts DateTime('UTC'), k UInt64) ENGINE = MergeTree ORDER BY ts;
CREATE TABLE t_r_mt (ts DateTime('UTC'), k UInt64) ENGINE = MergeTree ORDER BY ts;
INSERT INTO t_l_mt SELECT * FROM t_l;
INSERT INTO t_r_mt SELECT * FROM t_r;
SELECT count(), sum(l.k), sum(r.k) FROM t_l_mt AS l INNER JOIN t_r_mt AS r ON toDate(l.ts) = toDate(r.ts) WHERE toDate(l.ts) >= '2020-01-05';
SELECT countIf(explain LIKE '%Condition: (toDate(ts) in %') FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_l_mt AS l INNER JOIN t_r_mt AS r ON toDate(l.ts) = toDate(r.ts) WHERE toDate(l.ts) >= '2020-01-05'
);

DROP TABLE t_l;
DROP TABLE t_r;
DROP TABLE t_l_mt;
DROP TABLE t_r_mt;
