-- A key that a FULL JOIN filled with the default is not a value of the merged key of a following JOIN USING.

CREATE TABLE t1 (k UInt8, a String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t2 (k UInt8, b String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t3 (k Nullable(UInt8), c String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t1 VALUES (0, 'a0'), (1, 'a1'), (3, 'a3');
INSERT INTO t2 VALUES (0, 'b0'), (2, 'b2'), (3, 'b3');
INSERT INTO t3 VALUES (0, 'c0'), (2, 'c2'), (3, 'c3'), (NULL, 'cn');

SELECT 'INNER';
SELECT k, t1.k, t2.k, a, b, c FROM t1 FULL JOIN t2 USING (k) INNER JOIN t3 USING (k) ORDER BY c;
SELECT 'LEFT';
SELECT k, t1.k, t2.k, a, b, c FROM t1 FULL JOIN t2 USING (k) LEFT JOIN t3 USING (k) ORDER BY a, b;
SELECT 'FULL';
SELECT k, t1.k, t2.k, a, b, c FROM t1 FULL JOIN t2 USING (k) FULL JOIN t3 USING (k) ORDER BY a, b, c;
SELECT 'RIGHT';
SELECT k, t1.k, t2.k, a, b, c FROM t1 FULL JOIN t2 USING (k) RIGHT JOIN t3 USING (k) ORDER BY c;
SELECT 'WHERE';
SELECT count() FROM t1 FULL JOIN t2 USING (k) INNER JOIN t3 USING (k) WHERE k = 2;
SELECT 'INNER JOIN in between';
SELECT k FROM t1 FULL JOIN t2 USING (k) INNER JOIN t2 AS t4 USING (k) INNER JOIN t3 USING (k) ORDER BY k;
SELECT 'ON in between';
SELECT k FROM t1 FULL JOIN t2 USING (k) INNER JOIN (SELECT 2 AS x) AS t4 ON t4.x = k INNER JOIN t3 USING (k);
SELECT 'String, LowCardinality, Tuple';
SELECT k FROM (SELECT 'x' AS k) AS s1 FULL JOIN (SELECT 'y' AS k) AS s2 USING (k) INNER JOIN (SELECT 'y'::Nullable(String) AS k) AS s3 USING (k);
SELECT k FROM (SELECT toLowCardinality('x') AS k) AS s1 FULL JOIN (SELECT toLowCardinality('y') AS k) AS s2 USING (k) INNER JOIN (SELECT 'y'::Nullable(String) AS k) AS s3 USING (k);
SELECT k FROM (SELECT tuple(1::UInt8) AS k) AS s1 FULL JOIN (SELECT tuple(2::UInt8) AS k) AS s2 USING (k) INNER JOIN (SELECT tuple(2)::Tuple(Nullable(UInt8)) AS k) AS s3 USING (k);
SELECT 'join_use_nulls = 1';
SELECT k, t1.k, t2.k, a, b, c FROM t1 FULL JOIN t2 USING (k) INNER JOIN t3 USING (k) ORDER BY c SETTINGS join_use_nulls = 1;
