-- Random settings limits: optimize_or_like_chain=(1, None)

-- an `indexHint` in ON of a LEFT JOIN must not stop `full_sorting_merge`: it is always 1 and reads no column

SET join_algorithm = 'full_sorting_merge';

DROP TABLE IF EXISTS t_or_like_join_l;
DROP TABLE IF EXISTS t_or_like_join_r;
CREATE TABLE t_or_like_join_l (n UInt64, s String) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t_or_like_join_r (n UInt64, v String) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_or_like_join_l VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
INSERT INTO t_or_like_join_r VALUES (1, 'x'), (2, 'y'), (3, 'z'), (4, 'w');

SELECT 'or like chain';
SELECT l.s, r.v FROM t_or_like_join_l AS l LEFT JOIN t_or_like_join_r AS r ON l.n = r.n AND (r.v LIKE '%x%' OR r.v LIKE '%y%' OR r.v LIKE '%q%' OR r.v LIKE '%r%') ORDER BY l.s;

SELECT 'explicit indexHint';
SELECT l.s, r.v FROM t_or_like_join_l AS l LEFT JOIN t_or_like_join_r AS r ON l.n = r.n AND indexHint(r.v = 'x') ORDER BY l.s;

SELECT 'global right join swapped to left';
SELECT l.s, r.v FROM remote('127.0.0.{1,2}', currentDatabase(), t_or_like_join_l) AS l GLOBAL RIGHT JOIN t_or_like_join_r AS r ON l.n = r.n AND (l.s LIKE '%a%' OR l.s LIKE '%b%' OR l.s LIKE '%q%' OR l.s LIKE '%r%') ORDER BY r.v, l.s;

DROP TABLE t_or_like_join_l;
DROP TABLE t_or_like_join_r;
