-- Removes the always-true `x = x` filters that the equality substitution of correlated subqueries left on the inner table.
-- https://github.com/ClickHouse/ClickHouse/issues/122677
-- Random settings limits: query_plan_optimize_join_order_limit=(1, None); use_statistics=(1, 1); query_plan_optimize_join_order_randomize=(0, 0)
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (k Int64, c1 Int64, c2 Int64, c3 Int64, c4 Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t2 (k Int64, c1 Int64, c2 Int64, c3 Int64, c4 Int64, d Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t1 SELECT number, number % 3, number % 2, 0, 0 FROM numbers(10);
INSERT INTO t2 VALUES (1, 1, 1, 0, 0, 1), (2, 2, 0, 0, 0, 0), (3, 0, 1, 0, 1, 5), (5, 5, 1, 0, 0, 2), (7, 1, 1, 0, 0, 0);

SELECT '-- Int64 keys';
SELECT k FROM t1 AS a WHERE EXISTS (SELECT 1 FROM t2 AS b WHERE a.k = b.k AND a.c1 = b.c1 AND a.c2 = b.c2 AND a.c3 = b.c3 AND a.c4 = b.c4) ORDER BY k;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT k FROM t1 AS a WHERE EXISTS (SELECT 1 FROM t2 AS b WHERE a.k = b.k AND a.c1 = b.c1 AND a.c2 = b.c2 AND a.c3 = b.c3 AND a.c4 = b.c4));
SELECT k FROM t1 AS a WHERE NOT EXISTS (SELECT 1 FROM t2 AS b WHERE a.k = b.k) ORDER BY k;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT k FROM t1 AS a WHERE NOT EXISTS (SELECT 1 FROM t2 AS b WHERE a.k = b.k));

SELECT '-- scalar subquery';
SELECT k FROM t1 AS a WHERE a.c1 + 1 > (SELECT min(c1) FROM t2 AS b WHERE b.k = a.k) ORDER BY k;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT k FROM t1 AS a WHERE a.c1 + 1 > (SELECT min(c1) FROM t2 AS b WHERE b.k = a.k));

SELECT '-- two members of one class: only the absorbed equality goes';
SELECT k FROM t1 AS a WHERE EXISTS (SELECT 1 FROM t2 AS b WHERE b.k = a.k AND b.c1 = a.k) ORDER BY k;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT k FROM t1 AS a WHERE EXISTS (SELECT 1 FROM t2 AS b WHERE b.k = a.k AND b.c1 = a.k));

SELECT '-- the remaining conjuncts keep their order';
SELECT countIf(explain ILIKE '%FUNCTION and(notEquals(%'), countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT k FROM t1 AS a WHERE EXISTS (SELECT 1 FROM t2 AS b WHERE b.d != 0 AND b.k = a.k AND intDiv(100, b.d) > 0));

DROP TABLE IF EXISTS n1;
DROP TABLE IF EXISTS n2;
DROP TABLE IF EXISTS s1;
DROP TABLE IF EXISTS s2;
CREATE TABLE n1 (k Nullable(Int64), id Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE n2 (k Nullable(Int64), v Int64) ENGINE = MergeTree ORDER BY v;
INSERT INTO n1 VALUES (1, 1), (NULL, 2), (3, 3);
INSERT INTO n2 VALUES (1, 10), (NULL, 20), (NULL, 21), (4, 40);
CREATE TABLE s1 (k LowCardinality(Nullable(String)), id Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE s2 (k LowCardinality(Nullable(String)), v Int64) ENGINE = MergeTree ORDER BY v;
INSERT INTO s1 VALUES ('a', 1), (NULL, 2), ('c', 3);
INSERT INTO s2 VALUES ('a', 10), (NULL, 20), (NULL, 21), ('d', 40);

SELECT '-- Nullable keys keep their equality: it rejects NULL';
SELECT id FROM n1 AS a WHERE EXISTS (SELECT 1 FROM n2 AS b WHERE a.k = b.k) ORDER BY id;
SELECT id FROM n1 AS a WHERE NOT EXISTS (SELECT 1 FROM n2 AS b WHERE a.k = b.k) ORDER BY id;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT id FROM n1 AS a WHERE EXISTS (SELECT 1 FROM n2 AS b WHERE a.k = b.k));
SELECT a.id, l.w FROM n1 AS a LEFT JOIN LATERAL (SELECT b.v AS w FROM n2 AS b WHERE b.k = a.k) AS l ON true ORDER BY a.id, l.w;
SELECT a.id, l.c FROM n1 AS a LEFT JOIN LATERAL (SELECT count() AS c FROM n2 AS b WHERE b.k = a.k) AS l ON true ORDER BY a.id;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT a.id, l.w FROM n1 AS a LEFT JOIN LATERAL (SELECT b.v AS w FROM n2 AS b WHERE b.k = a.k) AS l ON true);
SELECT a.id, l.w FROM s1 AS a LEFT JOIN LATERAL (SELECT b.v AS w FROM s2 AS b WHERE b.k = a.k) AS l ON true ORDER BY a.id, l.w;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT a.id, l.w FROM s1 AS a LEFT JOIN LATERAL (SELECT b.v AS w FROM s2 AS b WHERE b.k = a.k) AS l ON true);

DROP TABLE IF EXISTS i32;
DROP TABLE IF EXISTS i64;
DROP TABLE IF EXISTS u8;
DROP TABLE IF EXISTS lc;
DROP TABLE IF EXISTS str;
CREATE TABLE i32 (k Int32, v Int64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE i64 (k Int64, v Int64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE u8 (k UInt8, id Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE lc (k LowCardinality(String), v Int64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE str (k String, id Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO i32 VALUES (1, 10), (4, 40);
INSERT INTO i64 VALUES (1, 10), (300, 300);
INSERT INTO u8 VALUES (1, 1), (44, 2);
INSERT INTO lc VALUES ('a', 10), ('d', 40);
INSERT INTO str VALUES ('a', 1), ('b', 2);

SELECT '-- lossless conversions of the inner key';
SELECT k FROM t1 AS a WHERE EXISTS (SELECT 1 FROM i32 AS b WHERE b.k = a.k) ORDER BY k;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT k FROM t1 AS a WHERE EXISTS (SELECT 1 FROM i32 AS b WHERE b.k = a.k));
SELECT id FROM str AS a WHERE EXISTS (SELECT 1 FROM lc AS b WHERE b.k = a.k) ORDER BY id;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT id FROM str AS a WHERE EXISTS (SELECT 1 FROM lc AS b WHERE b.k = a.k));
SELECT id FROM n1 AS a WHERE EXISTS (SELECT 1 FROM t2 AS b WHERE b.k = a.k) ORDER BY id;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT id FROM n1 AS a WHERE EXISTS (SELECT 1 FROM t2 AS b WHERE b.k = a.k));

SELECT '-- conversions that can reject rows keep the equality';
SELECT k FROM t1 AS a WHERE EXISTS (SELECT 1 FROM n2 AS b WHERE b.k = a.k) ORDER BY k;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT k FROM t1 AS a WHERE EXISTS (SELECT 1 FROM n2 AS b WHERE b.k = a.k));
SELECT id FROM u8 AS a WHERE EXISTS (SELECT 1 FROM i64 AS b WHERE b.k = a.k) ORDER BY id;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT id FROM u8 AS a WHERE EXISTS (SELECT 1 FROM i64 AS b WHERE b.k = a.k));

DROP TABLE IF EXISTS tf1;
DROP TABLE IF EXISTS tf2;
DROP TABLE IF EXISTS tn1;
DROP TABLE IF EXISTS tn2;
CREATE TABLE tf1 (id Int64, t Tuple(Float64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tf2 (id Int64, t Tuple(Float64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tn1 (id Int64, t Tuple(Nullable(Int64))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tn2 (id Int64, t Tuple(Nullable(Int64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tf1 VALUES (1, tuple(1.)), (2, tuple(nan));
INSERT INTO tf2 VALUES (10, tuple(1.)), (20, tuple(nan));
INSERT INTO tn1 VALUES (1, tuple(1)), (2, tuple(NULL));
INSERT INTO tn2 VALUES (10, tuple(1)), (20, tuple(NULL));

SELECT '-- Tuple keys keep the equality: x = x is not true for NaN or NULL elements';
SELECT id FROM tf1 AS a WHERE EXISTS (SELECT 1 FROM tf2 AS b WHERE a.t = b.t) ORDER BY id;
SELECT id FROM tn1 AS a WHERE EXISTS (SELECT 1 FROM tn2 AS b WHERE a.t = b.t) ORDER BY id;
SELECT countIf(explain ILIKE '%FUNCTION equals(%') FROM (EXPLAIN actions = 1, optimize = 0
    SELECT id FROM tf1 AS a WHERE EXISTS (SELECT 1 FROM tf2 AS b WHERE a.t = b.t));

DROP TABLE IF EXISTS big1;
DROP TABLE IF EXISTS big2;
CREATE TABLE big1 (k Int64, c1 Int64, c2 Int64, c3 Int64, c4 Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE big2 (k Int64, c1 Int64, c2 Int64, c3 Int64, c4 Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO big1 SELECT number, number % 10, number % 7, number % 3, number % 2 FROM numbers(10000);
INSERT INTO big2 SELECT number, number % 10, number % 7, number % 3, number % 2 FROM numbers(10000);

SELECT '-- the inner relation is estimated at its full size';
SELECT count() FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM big1 AS a WHERE EXISTS (SELECT 1 FROM big2 AS b WHERE a.k = b.k AND a.c1 = b.c1 AND a.c2 = b.c2 AND a.c3 = b.c3 AND a.c4 = b.c4)
    SETTINGS collect_hash_table_stats_during_joins = 0)
WHERE match(explain, 'big2\\[(no_stats~)?10000\\]');

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE n1;
DROP TABLE n2;
DROP TABLE s1;
DROP TABLE s2;
DROP TABLE i32;
DROP TABLE i64;
DROP TABLE u8;
DROP TABLE lc;
DROP TABLE str;
DROP TABLE tf1;
DROP TABLE tf2;
DROP TABLE tn1;
DROP TABLE tn2;
DROP TABLE big1;
DROP TABLE big2;
