-- A direct join with a MergeTree right table fills the right columns of not-found keys with the type default, like the
-- hash join: for an `Enum` that is its first value, not 0.

SET join_algorithm = 'direct';
SET join_use_nulls = 0;

DROP TABLE IF EXISTS t_direct_enum_right;

CREATE TABLE t_direct_enum_right (k UInt64, e Enum8('a' = 1, 'b' = 2), e16 Enum16('x' = -300, 'y' = 7), t Tuple(s String, e Enum16('x' = -5, 'y' = 7)), j JSON(p Enum8('a' = 1, 'b' = 2)), u UInt8)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_direct_enum_right VALUES (1, 'b', 'y', ('r1', 'y'), '{"p":"b"}', 9);

-- A lookup with a found and a not-found key, then one where no key is found.
SELECT 'left';
SELECT l.k, r.e, r.e16, r.t, r.j, r.u FROM (SELECT number + 1 AS k FROM numbers(2)) AS l LEFT JOIN t_direct_enum_right AS r ON l.k = r.k ORDER BY l.k;
SELECT l.k, r.e, r.e16, r.t, r.j, r.u FROM (SELECT number + 7 AS k FROM numbers(2)) AS l LEFT JOIN t_direct_enum_right AS r ON l.k = r.k ORDER BY l.k;

SELECT 'left anti';
SELECT l.k, r.e, r.e16, r.t, r.j, r.u FROM (SELECT number + 1 AS k FROM numbers(2)) AS l LEFT ANTI JOIN t_direct_enum_right AS r ON l.k = r.k ORDER BY l.k;
SELECT l.k, r.e, r.e16, r.t, r.j, r.u FROM (SELECT number + 7 AS k FROM numbers(2)) AS l LEFT ANTI JOIN t_direct_enum_right AS r ON l.k = r.k ORDER BY l.k;

SELECT 'filter';
SELECT count() FROM (SELECT number + 1 AS k FROM numbers(2)) AS l LEFT JOIN t_direct_enum_right AS r ON l.k = r.k WHERE r.e = 'a' AND r.e16 = 'x' AND r.t.e = 'x';

DROP TABLE t_direct_enum_right;
