-- arrayJoin in a correlated subquery

DROP TABLE IF EXISTS t_outer;
CREATE TABLE t_outer (k UInt32, a Array(UInt32)) ENGINE = Memory;
INSERT INTO t_outer VALUES (1, [1, 2, 3]), (2, [2]), (3, [4, 4]);

SELECT k, (SELECT sum(arrayJoin(t_outer.a))) FROM t_outer ORDER BY k;
SELECT k, (SELECT count() FROM numbers(5) WHERE number = arrayJoin(t_outer.a)) FROM t_outer ORDER BY k;

DROP TABLE t_outer;
