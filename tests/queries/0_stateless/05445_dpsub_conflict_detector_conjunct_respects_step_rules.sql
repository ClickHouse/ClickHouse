-- `a1.y = a2.y` rejects NULLs on the `Nullable` key `a1.y`, so `a0 LEFT JOIN a1 LEFT JOIN a2` may be regrouped
-- as `a0 LEFT JOIN (a1 LEFT JOIN a2)`. The conjunct `a2.v = a3.v` of the inner join must then not join `a3`
-- to `a1 LEFT JOIN a2` on its own: that applies part of the inner join inside the padded side of
-- `a0 LEFT JOIN a1`, against the conflict rules of the inner join, and `a1.k = a3.k` later passes on the
-- padded `0 = 0`, returning rows that the inner join must not return.
-- Every query must return the same rows as without reordering.

SET enable_analyzer = 1;
SET join_use_nulls = 0;
SET query_plan_optimize_join_order_algorithm = 'dpsub,greedy';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET use_hash_table_stats_for_join_reordering = 0;

DROP TABLE IF EXISTS a0;
DROP TABLE IF EXISTS a1;
DROP TABLE IF EXISTS a2;
DROP TABLE IF EXISTS a3;

CREATE TABLE a0 (k UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE a1 (k UInt8, y Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE a2 (y Nullable(UInt8), v UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE a3 (k UInt8, v Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO a0 VALUES (2), (1), (0);
INSERT INTO a1 VALUES (3, NULL);
INSERT INTO a2 VALUES (NULL, 3), (2, 0), (0, 0), (NULL, 0);
INSERT INTO a3 VALUES (0, 1);

SELECT '-- CD-C';
SELECT a0.k, a1.k, a2.v, a3.k, a3.v
FROM a0 LEFT JOIN a1 ON a0.k = a1.k
LEFT JOIN a2 ON a1.y = a2.y
INNER JOIN a3 ON a1.k = a3.k AND a2.v = a3.v
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_conflict_detector = 'c';

SELECT '-- CD-A';
SELECT a0.k, a1.k, a2.v, a3.k, a3.v
FROM a0 LEFT JOIN a1 ON a0.k = a1.k
LEFT JOIN a2 ON a1.y = a2.y
INNER JOIN a3 ON a1.k = a3.k AND a2.v = a3.v
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_conflict_detector = 'a';

-- The same with a row that the inner join does return.
INSERT INTO a1 VALUES (1, 2);
INSERT INTO a3 VALUES (1, 0);

SELECT '-- CD-C, matching row';
SELECT a0.k, a1.k, a2.v, a3.k, a3.v
FROM a0 LEFT JOIN a1 ON a0.k = a1.k
LEFT JOIN a2 ON a1.y = a2.y
INNER JOIN a3 ON a1.k = a3.k AND a2.v = a3.v
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_conflict_detector = 'c';

DROP TABLE a0;
DROP TABLE a1;
DROP TABLE a2;
DROP TABLE a3;
