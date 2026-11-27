-- Two set subqueries of one mutation stage are planned separately, so the same `IN` subquery inside both of
-- them gives two sets with the same key in the `PreparedSetsCache` of a plain `MergeTree` mutation. The second
-- one waited for the first in the same executor, which a mutation runs with one thread: the mutation never finished.
-- Two parts that build the same sets in opposite orders waited for each other the same way.
-- The right table is not a `MergeTree`, so the inner sets are not built in place for index analysis.

DROP TABLE IF EXISTS t_same_inner_set;
DROP TABLE IF EXISTS r_same_inner_set;

CREATE TABLE t_same_inner_set (k UInt64, v UInt8, w UInt8) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r_same_inner_set (k UInt64) ENGINE = Memory;
INSERT INTO r_same_inner_set SELECT number FROM numbers(4);

SET mutations_sync = 2;

-- Two assignments.
INSERT INTO t_same_inner_set SELECT number, 0, 0 FROM numbers(4);
ALTER TABLE t_same_inner_set UPDATE
    v = k IN (SELECT k FROM r_same_inner_set WHERE k IN (SELECT number FROM numbers(2))),
    w = k IN (SELECT k FROM r_same_inner_set WHERE k IN (SELECT number FROM numbers(2)) AND k > 0)
WHERE 1;
SELECT k, v, w FROM t_same_inner_set ORDER BY k;

-- One assignment.
TRUNCATE TABLE t_same_inner_set;
INSERT INTO t_same_inner_set SELECT number, 0, 0 FROM numbers(4);
ALTER TABLE t_same_inner_set UPDATE
    v = k IN (SELECT k FROM r_same_inner_set WHERE k IN (SELECT number FROM numbers(2)) AND k > 0)
        OR k IN (SELECT k FROM r_same_inner_set WHERE k IN (SELECT number FROM numbers(2)) AND k < 1)
WHERE 1;
SELECT k, v, w FROM t_same_inner_set ORDER BY k;

-- Two sets with the same inner `IN` subquery inside the subquery of one set.
TRUNCATE TABLE t_same_inner_set;
INSERT INTO t_same_inner_set SELECT number, 0, 0 FROM numbers(4);
ALTER TABLE t_same_inner_set UPDATE
    w = k IN (SELECT k FROM r_same_inner_set
        WHERE k IN (SELECT k FROM r_same_inner_set WHERE k IN (SELECT number FROM numbers(3)) AND k > 0)
            OR k IN (SELECT k FROM r_same_inner_set WHERE k IN (SELECT number FROM numbers(3)) AND k > 1))
WHERE 1;
SELECT k, v, w FROM t_same_inner_set ORDER BY k;

DROP TABLE t_same_inner_set;

-- Several parts share the sets of the mutation. The parts are in different partitions, so they are not merged.
CREATE TABLE t_same_inner_set (k UInt64, v UInt8, w UInt8) ENGINE = MergeTree PARTITION BY k % 3 ORDER BY k;
INSERT INTO t_same_inner_set SELECT number, 0, 0 FROM numbers(12);
ALTER TABLE t_same_inner_set UPDATE
    v = k IN (SELECT k FROM r_same_inner_set WHERE k IN (SELECT number FROM numbers(2))),
    w = k IN (SELECT k FROM r_same_inner_set WHERE k IN (SELECT number FROM numbers(2)) AND k > 0)
WHERE 1;
SELECT count(), sum(v), sum(w) FROM t_same_inner_set;

DROP TABLE t_same_inner_set;

-- Two parts build the same two sets in opposite orders: each part built one of them and waited for the other.
CREATE TABLE t_same_inner_set (p UInt8, k UInt64, v UInt8, w UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO t_same_inner_set SELECT number % 2, number, 0, 0 FROM numbers(16);
TRUNCATE TABLE r_same_inner_set;
INSERT INTO r_same_inner_set SELECT number FROM numbers(200000);
ALTER TABLE t_same_inner_set
    UPDATE
        v = k IN (SELECT k FROM r_same_inner_set WHERE k % 2 = 0 SETTINGS max_block_size = 1024),
        w = k IN (SELECT k FROM r_same_inner_set WHERE k % 3 = 0 SETTINGS max_block_size = 1024)
    IN PARTITION 0 WHERE 1,
    UPDATE
        v = k IN (SELECT k FROM r_same_inner_set WHERE k % 3 = 0 SETTINGS max_block_size = 1024),
        w = k IN (SELECT k FROM r_same_inner_set WHERE k % 2 = 0 SETTINGS max_block_size = 1024)
    IN PARTITION 1 WHERE 1;
SELECT p, sum(v), sum(w) FROM t_same_inner_set GROUP BY p ORDER BY p;

DROP TABLE t_same_inner_set;
DROP TABLE r_same_inner_set;
