-- `allow_experimental_lateral_join` is an alias of `enable_lateral_join`.

DROP TABLE IF EXISTS t_lateral_left;
DROP TABLE IF EXISTS t_lateral_right;
CREATE TABLE t_lateral_left (id UInt32) ENGINE = Memory;
CREATE TABLE t_lateral_right (id UInt32, v String) ENGINE = Memory;
INSERT INTO t_lateral_left VALUES (1), (2);
INSERT INTO t_lateral_right VALUES (1, 'a'), (1, 'b'), (3, 'c');

SET enable_lateral_join = 1;
SELECT l.id, r.v FROM t_lateral_left AS l
INNER JOIN LATERAL (SELECT v FROM t_lateral_right AS t WHERE t.id = l.id) AS r ON true
ORDER BY l.id, r.v;

SET allow_experimental_lateral_join = 0;
SELECT l.id, r.v FROM t_lateral_left AS l
INNER JOIN LATERAL (SELECT v FROM t_lateral_right AS t WHERE t.id = l.id) AS r ON true; -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_lateral_join = 1;
SET enable_lateral_join = 0;
SELECT l.id, r.v FROM t_lateral_left AS l
INNER JOIN LATERAL (SELECT v FROM t_lateral_right AS t WHERE t.id = l.id) AS r ON true; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_lateral_left;
DROP TABLE t_lateral_right;
