-- Unmatched rows of a block nested loop join get the type default of the padded columns, which for an
-- `Enum` is its smallest value and not the column's zero (issue #130556).

SET join_use_nulls = 0;
SET allow_block_nested_loop_join = 1;

DROP TABLE IF EXISTS bnl_enum_l;
DROP TABLE IF EXISTS bnl_enum_r;

CREATE TABLE bnl_enum_l (id UInt8, e Enum8('a' = 1, 'b' = 2), t Tuple(s String, e Enum8('a' = 1, 'b' = 2))) ENGINE = Memory;
CREATE TABLE bnl_enum_r (id UInt8, e Enum8('a' = 1, 'b' = 2), t Tuple(s String, e Enum8('a' = 1, 'b' = 2))) ENGINE = Memory;
INSERT INTO bnl_enum_l VALUES (1, 'a', ('x', 'b')), (2, 'b', ('y', 'b'));
INSERT INTO bnl_enum_r VALUES (10, 'a', ('z', 'b')), (20, 'b', ('w', 'b'));

-- The condition is false for every pair, so every row of both sides is unmatched.
SELECT 'routed', count() FROM (EXPLAIN SELECT * FROM bnl_enum_l AS l FULL JOIN bnl_enum_r AS r ON length(toString(l.e)) > length(toString(r.e)))
WHERE explain LIKE '%BlockNestedLoopJoin%';

SELECT 'left';
SELECT l.id, r.id, r.e, CAST(r.e AS Int8), r.t FROM bnl_enum_l AS l LEFT JOIN bnl_enum_r AS r ON length(toString(l.e)) > length(toString(r.e)) ORDER BY l.id;
SELECT 'right';
SELECT l.id, l.e, CAST(l.e AS Int8), l.t, r.id FROM bnl_enum_l AS l RIGHT JOIN bnl_enum_r AS r ON length(toString(l.e)) > length(toString(r.e)) ORDER BY r.id;
SELECT 'full';
SELECT l.id, l.e, r.id, r.e FROM bnl_enum_l AS l FULL JOIN bnl_enum_r AS r ON length(toString(l.e)) > length(toString(r.e)) ORDER BY l.id, r.id;
SELECT 'left anti';
SELECT l.id, r.id, r.e FROM bnl_enum_l AS l ANTI LEFT JOIN bnl_enum_r AS r ON length(toString(l.e)) > length(toString(r.e)) ORDER BY l.id;
SELECT 'right anti';
SELECT l.id, l.e, r.id FROM bnl_enum_l AS l ANTI RIGHT JOIN bnl_enum_r AS r ON length(toString(l.e)) > length(toString(r.e)) ORDER BY r.id;
SELECT 'filter on the padded column';
SELECT count() FROM bnl_enum_l AS l LEFT JOIN bnl_enum_r AS r ON length(toString(l.e)) > length(toString(r.e)) WHERE r.e = 'a';
SELECT count() FROM bnl_enum_l AS l RIGHT JOIN bnl_enum_r AS r ON length(toString(l.e)) > length(toString(r.e)) WHERE l.e = 'a';
SELECT 'join_use_nulls = 1';
SELECT l.id, r.e FROM bnl_enum_l AS l LEFT JOIN bnl_enum_r AS r ON length(toString(l.e)) > length(toString(r.e)) ORDER BY l.id SETTINGS join_use_nulls = 1;

DROP TABLE bnl_enum_l;
DROP TABLE bnl_enum_r;
