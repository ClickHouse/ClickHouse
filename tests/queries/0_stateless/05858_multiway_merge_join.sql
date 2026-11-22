-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN shows another plan with parallel replicas.

-- A subtree of full_sorting_merge inner joins on one key runs as one MultiwayMergeJoin step.
-- Every case returns the same summary with query_plan_join_multiway_merge = 0 and 1, and checks that the step is used.

SET join_algorithm = 'full_sorting_merge', max_rows_in_set_to_optimize_join = 0, optimize_read_in_order = 1, max_threads = 4;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;
DROP TABLE IF EXISTS t_even;
DROP TABLE IF EXISTS t_three;
DROP TABLE IF EXISTS d1;
DROP TABLE IF EXISTS d2;
DROP TABLE IF EXISTS d3;
DROP TABLE IF EXISTS w;
DROP TABLE IF EXISTS n1;
DROP TABLE IF EXISTS n2;
DROP TABLE IF EXISTS n3;
DROP TABLE IF EXISTS m1;
DROP TABLE IF EXISTS m2;
DROP TABLE IF EXISTS m3;
DROP TABLE IF EXISTS s1;
DROP TABLE IF EXISTS s2;
DROP TABLE IF EXISTS s3;
DROP TABLE IF EXISTS e;

CREATE TABLE t1 (id UInt64, v1 UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt64, v2 UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t3 (id UInt64, v3 UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t4 (id UInt64, v4 UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t5 (id UInt64, v5 UInt64) ENGINE = MergeTree ORDER BY id;
-- Three parts per table.
INSERT INTO t1 SELECT number, number * 3 + 1 FROM numbers(30000) WHERE number % 3 = 0;
INSERT INTO t1 SELECT number, number * 3 + 1 FROM numbers(30000) WHERE number % 3 = 1;
INSERT INTO t1 SELECT number, number * 3 + 1 FROM numbers(30000) WHERE number % 3 = 2;
INSERT INTO t2 SELECT number, number * 5 + 2 FROM numbers(30000) WHERE number % 3 = 0;
INSERT INTO t2 SELECT number, number * 5 + 2 FROM numbers(30000) WHERE number % 3 = 1;
INSERT INTO t2 SELECT number, number * 5 + 2 FROM numbers(30000) WHERE number % 3 = 2;
INSERT INTO t3 SELECT number, number * 7 + 3 FROM numbers(30000) WHERE number < 10000;
INSERT INTO t3 SELECT number, number * 7 + 3 FROM numbers(30000) WHERE number >= 10000 AND number < 20000;
INSERT INTO t3 SELECT number, number * 7 + 3 FROM numbers(30000) WHERE number >= 20000;
INSERT INTO t4 SELECT number, number * 11 + 4 FROM numbers(30000) WHERE number % 2 = 0;
INSERT INTO t4 SELECT number, number * 11 + 4 FROM numbers(30000) WHERE number % 2 = 1;
INSERT INTO t4 SELECT number + 30000, number FROM numbers(100);
INSERT INTO t5 SELECT number, number * 13 + 5 FROM numbers(30000) WHERE number % 5 = 0;
INSERT INTO t5 SELECT number, number * 13 + 5 FROM numbers(30000) WHERE number % 5 IN (1, 2);
INSERT INTO t5 SELECT number, number * 13 + 5 FROM numbers(30000) WHERE number % 5 >= 3;

SELECT '1. aligned unique keys';
SELECT count(), sum(cityHash64(id, v1, v2, v3, v4, v5)) FROM t1 JOIN t2 USING (id) JOIN t3 USING (id) JOIN t4 USING (id) JOIN t5 USING (id) SETTINGS query_plan_join_multiway_merge = 0;
SELECT count(), sum(cityHash64(id, v1, v2, v3, v4, v5)) FROM t1 JOIN t2 USING (id) JOIN t3 USING (id) JOIN t4 USING (id) JOIN t5 USING (id) SETTINGS query_plan_join_multiway_merge = 1;
SELECT count(), sum(cityHash64(id, v1, v2, v3, v4, v5)) FROM t1 JOIN t2 USING (id) JOIN t3 USING (id) JOIN t4 USING (id) JOIN t5 USING (id) SETTINGS query_plan_join_multiway_merge = 1, query_plan_join_shard_by_pk_ranges = 0;
SELECT countIf(explain ILIKE '%MultiwayMergeJoin%'), countIf(explain ILIKE '%Sharding%')
FROM (EXPLAIN SELECT count(), sum(cityHash64(id, v1, v2, v3, v4, v5)) FROM t1 JOIN t2 USING (id) JOIN t3 USING (id) JOIN t4 USING (id) JOIN t5 USING (id)
    SETTINGS query_plan_join_multiway_merge = 1, query_plan_join_shard_by_pk_ranges = 1);

CREATE TABLE t_even (id UInt64, ve UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_three (id UInt64, vt UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_even SELECT number * 2, number FROM numbers(15000) WHERE number % 2 = 0;
INSERT INTO t_even SELECT number * 2, number FROM numbers(15000) WHERE number % 2 = 1;
INSERT INTO t_three SELECT number * 3, number FROM numbers(10000) WHERE number < 5000;
INSERT INTO t_three SELECT number * 3, number FROM numbers(10000) WHERE number >= 5000;

SELECT '2. sparse and missing keys';
SELECT count(), sum(cityHash64(id, v1, ve, vt, v2)) FROM t1 JOIN t_even USING (id) JOIN t_three USING (id) JOIN t2 USING (id) SETTINGS query_plan_join_multiway_merge = 0;
SELECT count(), sum(cityHash64(id, v1, ve, vt, v2)) FROM t1 JOIN t_even USING (id) JOIN t_three USING (id) JOIN t2 USING (id) SETTINGS query_plan_join_multiway_merge = 1;
SELECT count(), sum(cityHash64(id, v1, ve, vt, v2)) FROM t1 JOIN t_even USING (id) JOIN t_three USING (id) JOIN t2 USING (id) SETTINGS query_plan_join_multiway_merge = 1, query_plan_join_shard_by_pk_ranges = 0;
SELECT countIf(explain ILIKE '%MultiwayMergeJoin%')
FROM (EXPLAIN SELECT sum(cityHash64(id, v1, ve, vt, v2)) FROM t1 JOIN t_even USING (id) JOIN t_three USING (id) JOIN t2 USING (id) SETTINGS query_plan_join_multiway_merge = 1);

CREATE TABLE d1 (id UInt64, x1 UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE d2 (id UInt64, x2 UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE d3 (id UInt64, x3 UInt64) ENGINE = MergeTree ORDER BY id;
-- Every key three times in d1 and twice in d2. d3 has the keys of d1, with the same duplicates.
INSERT INTO d1 SELECT intDiv(number, 3) * 5, number FROM numbers(3000);
INSERT INTO d2 SELECT intDiv(number, 2) * 5, number FROM numbers(2000);
INSERT INTO d3 SELECT intDiv(number, 3) * 5, number + 7 FROM numbers(3000);

SELECT '3. duplicate keys';
SELECT count(), sum(cityHash64(id, x1, x2, v1)) FROM d1 JOIN d2 USING (id) JOIN t1 USING (id) SETTINGS query_plan_join_multiway_merge = 0;
SELECT count(), sum(cityHash64(id, x1, x2, v1)) FROM d1 JOIN d2 USING (id) JOIN t1 USING (id) SETTINGS query_plan_join_multiway_merge = 1;
SELECT count(), sum(cityHash64(id, x1, x2, v1)) FROM d1 JOIN d2 USING (id) JOIN t1 USING (id) SETTINGS query_plan_join_multiway_merge = 1, max_block_size = 3;
SELECT count(), sum(cityHash64(id, x1, x2, v1)) FROM d1 JOIN d2 USING (id) JOIN t1 USING (id) SETTINGS query_plan_join_multiway_merge = 1, max_block_size = 3, query_plan_join_shard_by_pk_ranges = 0;
SELECT count(), sum(cityHash64(id, x1, x3, x2)) FROM d1 JOIN d3 USING (id) JOIN d2 USING (id) SETTINGS query_plan_join_multiway_merge = 0;
SELECT count(), sum(cityHash64(id, x1, x3, x2)) FROM d1 JOIN d3 USING (id) JOIN d2 USING (id) SETTINGS query_plan_join_multiway_merge = 1;
SELECT count(), sum(cityHash64(id, x1, x3, x2)) FROM d1 JOIN d3 USING (id) JOIN d2 USING (id) SETTINGS query_plan_join_multiway_merge = 1, max_block_size = 3;
SELECT countIf(explain ILIKE '%MultiwayMergeJoin%')
FROM (EXPLAIN SELECT sum(cityHash64(id, x1, x3, x2)) FROM d1 JOIN d3 USING (id) JOIN d2 USING (id) SETTINGS query_plan_join_multiway_merge = 1);

CREATE TABLE w (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO w SELECT 1, number FROM numbers(4);

SELECT '3b. a product of 4^20 rows with a limit';
-- One step over all 20 inputs streams the product. A two-input join above a part of the chain would collect the
-- whole product of that part first, so the settings that can leave one (lazy materialization, join order) are pinned.
SELECT sum(x < 4) FROM (
    SELECT w1.x AS x FROM w AS w1 JOIN w AS w2 USING (k) JOIN w AS w3 USING (k) JOIN w AS w4 USING (k) JOIN w AS w5 USING (k)
        JOIN w AS w6 USING (k) JOIN w AS w7 USING (k) JOIN w AS w8 USING (k) JOIN w AS w9 USING (k) JOIN w AS w10 USING (k)
        JOIN w AS w11 USING (k) JOIN w AS w12 USING (k) JOIN w AS w13 USING (k) JOIN w AS w14 USING (k) JOIN w AS w15 USING (k)
        JOIN w AS w16 USING (k) JOIN w AS w17 USING (k) JOIN w AS w18 USING (k) JOIN w AS w19 USING (k) JOIN w AS w20 USING (k)
    LIMIT 5)
SETTINGS query_plan_join_multiway_merge = 1, query_plan_optimize_lazy_materialization = 0, query_plan_optimize_join_order_limit = 0;
SELECT countIf(explain ILIKE '%Inputs: 20%')
FROM (EXPLAIN SELECT sum(x < 4) FROM (
    SELECT w1.x AS x FROM w AS w1 JOIN w AS w2 USING (k) JOIN w AS w3 USING (k) JOIN w AS w4 USING (k) JOIN w AS w5 USING (k)
        JOIN w AS w6 USING (k) JOIN w AS w7 USING (k) JOIN w AS w8 USING (k) JOIN w AS w9 USING (k) JOIN w AS w10 USING (k)
        JOIN w AS w11 USING (k) JOIN w AS w12 USING (k) JOIN w AS w13 USING (k) JOIN w AS w14 USING (k) JOIN w AS w15 USING (k)
        JOIN w AS w16 USING (k) JOIN w AS w17 USING (k) JOIN w AS w18 USING (k) JOIN w AS w19 USING (k) JOIN w AS w20 USING (k)
    LIMIT 5)
    SETTINGS query_plan_join_multiway_merge = 1, query_plan_optimize_lazy_materialization = 0, query_plan_optimize_join_order_limit = 0);

CREATE TABLE n1 (id Nullable(UInt64), y1 UInt64) ENGINE = MergeTree ORDER BY id SETTINGS allow_nullable_key = 1;
CREATE TABLE n2 (id Nullable(UInt64), y2 UInt64) ENGINE = MergeTree ORDER BY id SETTINGS allow_nullable_key = 1;
CREATE TABLE n3 (id Nullable(UInt64), y3 UInt64) ENGINE = MergeTree ORDER BY id SETTINGS allow_nullable_key = 1;
INSERT INTO n1 SELECT if(number % 4 = 0, NULL, number), number FROM numbers(1000);
INSERT INTO n2 SELECT if(number % 3 = 0, NULL, number), number FROM numbers(1000);
INSERT INTO n3 SELECT if(number % 5 = 0, NULL, number), number FROM numbers(1000);

SELECT '4. nullable keys';
SELECT count(), sum(cityHash64(n1.id, y1, y2, y3)) FROM n1 JOIN n2 ON n1.id = n2.id JOIN n3 ON n2.id = n3.id SETTINGS query_plan_join_multiway_merge = 0;
SELECT count(), sum(cityHash64(n1.id, y1, y2, y3)) FROM n1 JOIN n2 ON n1.id = n2.id JOIN n3 ON n2.id = n3.id SETTINGS query_plan_join_multiway_merge = 1;
SELECT count(), sum(cityHash64(n1.id, y1, y2, y3)) FROM n1 JOIN n2 ON n1.id = n2.id JOIN n3 ON n2.id = n3.id SETTINGS query_plan_join_multiway_merge = 1, max_block_size = 3;
SELECT countIf(explain ILIKE '%MultiwayMergeJoin%')
FROM (EXPLAIN SELECT sum(cityHash64(n1.id, y1, y2, y3)) FROM n1 JOIN n2 ON n1.id = n2.id JOIN n3 ON n2.id = n3.id SETTINGS query_plan_join_multiway_merge = 1);

SELECT '5. a subquery input';
SELECT count(), sum(cityHash64(id, v1, s, v2)) FROM t1 JOIN (SELECT number * 4 AS id, number AS s FROM numbers(10000)) AS q USING (id) JOIN t2 USING (id)
SETTINGS query_plan_join_multiway_merge = 0;
SELECT count(), sum(cityHash64(id, v1, s, v2)) FROM t1 JOIN (SELECT number * 4 AS id, number AS s FROM numbers(10000)) AS q USING (id) JOIN t2 USING (id)
SETTINGS query_plan_join_multiway_merge = 1;
SELECT countIf(explain ILIKE '%MultiwayMergeJoin%')
FROM (EXPLAIN SELECT sum(cityHash64(id, v1, s, v2)) FROM t1 JOIN (SELECT number * 4 AS id, number AS s FROM numbers(10000)) AS q USING (id) JOIN t2 USING (id)
    SETTINGS query_plan_join_multiway_merge = 1);

CREATE TABLE m1 (a UInt32, b UInt64, z1 UInt64) ENGINE = MergeTree ORDER BY (a, b);
CREATE TABLE m2 (a UInt32, b UInt64, z2 UInt64) ENGINE = MergeTree ORDER BY (a, b);
CREATE TABLE m3 (a UInt32, b UInt64, z3 UInt64) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO m1 SELECT number % 10, intDiv(number, 10), number FROM numbers(5000);
INSERT INTO m2 SELECT number % 10, intDiv(number, 10) * 2, number FROM numbers(5000);
INSERT INTO m3 SELECT number % 7, intDiv(number, 7), number FROM numbers(5000);

SELECT '6. two key columns';
-- The join order optimizer may list the conditions of a join in another order, and then the inputs are not sorted alike.
SELECT count(), sum(cityHash64(m1.a, m1.b, z1, z2, z3)) FROM m1 JOIN m2 ON m1.a = m2.a AND m1.b = m2.b JOIN m3 ON m2.a = m3.a AND m2.b = m3.b
SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_join_multiway_merge = 0;
SELECT count(), sum(cityHash64(m1.a, m1.b, z1, z2, z3)) FROM m1 JOIN m2 ON m1.a = m2.a AND m1.b = m2.b JOIN m3 ON m2.a = m3.a AND m2.b = m3.b
SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_join_multiway_merge = 1;
SELECT count(), sum(cityHash64(m1.a, m1.b, z1, z2, z3)) FROM m1 JOIN m2 ON m1.a = m2.a AND m1.b = m2.b JOIN m3 ON m2.a = m3.a AND m2.b = m3.b
SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_join_multiway_merge = 1, query_plan_join_shard_by_pk_ranges = 0;
SELECT countIf(explain ILIKE '%MultiwayMergeJoin%')
FROM (EXPLAIN SELECT sum(cityHash64(m1.a, m1.b, z1, z2, z3)) FROM m1 JOIN m2 ON m1.a = m2.a AND m1.b = m2.b JOIN m3 ON m2.a = m3.a AND m2.b = m3.b
    SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_join_multiway_merge = 1);

CREATE TABLE s1 (k LowCardinality(String), u1 UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE s2 (k String, u2 UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE s3 (k LowCardinality(String), u3 UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO s1 SELECT toString(number % 300), number FROM numbers(900);
INSERT INTO s2 SELECT toString(number), number FROM numbers(500);
INSERT INTO s3 SELECT toString(number * 2), number FROM numbers(200);

SELECT '7. LowCardinality(String) and String keys';
-- In another join order a cast of a key would sit between two joins.
SELECT count(), sum(cityHash64(s1.k, u1, u2, u3)) FROM s1 JOIN s2 ON s1.k = s2.k JOIN s3 ON s2.k = s3.k
SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_join_multiway_merge = 0;
SELECT count(), sum(cityHash64(s1.k, u1, u2, u3)) FROM s1 JOIN s2 ON s1.k = s2.k JOIN s3 ON s2.k = s3.k
SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_join_multiway_merge = 1;
SELECT countIf(explain ILIKE '%MultiwayMergeJoin%')
FROM (EXPLAIN SELECT sum(cityHash64(s1.k, u1, u2, u3)) FROM s1 JOIN s2 ON s1.k = s2.k JOIN s3 ON s2.k = s3.k
    SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_join_multiway_merge = 1);

CREATE TABLE e (id UInt64, ev UInt64) ENGINE = MergeTree ORDER BY id;

SELECT '8. an empty input';
SELECT count(), sum(cityHash64(id, v1, ev, v2)) FROM t1 JOIN e USING (id) JOIN t2 USING (id) SETTINGS query_plan_join_multiway_merge = 1;
SELECT countIf(explain ILIKE '%MultiwayMergeJoin%')
FROM (EXPLAIN SELECT sum(cityHash64(id, v1, ev, v2)) FROM t1 JOIN e USING (id) JOIN t2 USING (id) SETTINGS query_plan_join_multiway_merge = 1);

SELECT '9. a LEFT JOIN above inner joins';
-- Another join order may join the inner tables on both sides of the LEFT JOIN.
SELECT count(), sum(cityHash64(id, v1, v2, v3, ve)) FROM t1 JOIN t2 USING (id) JOIN t3 USING (id) LEFT JOIN t_even USING (id)
SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_join_multiway_merge = 0;
SELECT count(), sum(cityHash64(id, v1, v2, v3, ve)) FROM t1 JOIN t2 USING (id) JOIN t3 USING (id) LEFT JOIN t_even USING (id)
SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_join_multiway_merge = 1;
SELECT countIf(explain ILIKE '%MultiwayMergeJoin%')
FROM (EXPLAIN SELECT sum(cityHash64(id, v1, v2, v3, ve)) FROM t1 JOIN t2 USING (id) JOIN t3 USING (id) LEFT JOIN t_even USING (id)
    SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_join_multiway_merge = 1);

SELECT '10. a self-join';
SELECT count(), sum(cityHash64(a.id, a.v4, b.v4, c.v4)) FROM t4 AS a JOIN t4 AS b USING (id) JOIN t4 AS c USING (id) SETTINGS query_plan_join_multiway_merge = 0;
SELECT count(), sum(cityHash64(a.id, a.v4, b.v4, c.v4)) FROM t4 AS a JOIN t4 AS b USING (id) JOIN t4 AS c USING (id) SETTINGS query_plan_join_multiway_merge = 1;
SELECT countIf(explain ILIKE '%MultiwayMergeJoin%')
FROM (EXPLAIN SELECT sum(cityHash64(a.id, a.v4, b.v4, c.v4)) FROM t4 AS a JOIN t4 AS b USING (id) JOIN t4 AS c USING (id) SETTINGS query_plan_join_multiway_merge = 1);

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t_even;
DROP TABLE t_three;
DROP TABLE d1;
DROP TABLE d2;
DROP TABLE d3;
DROP TABLE w;
DROP TABLE n1;
DROP TABLE n2;
DROP TABLE n3;
DROP TABLE m1;
DROP TABLE m2;
DROP TABLE m3;
DROP TABLE s1;
DROP TABLE s2;
DROP TABLE s3;
DROP TABLE e;
