-- A direct join with a MergeTree right table concatenates the lookup blocks read from all parts; Tuple elements that are
-- Sparse in one part must not break the concatenation with a part where they are dense.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_right (id UInt64, t Tuple(a UInt64, s String, n Tuple(b UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_right;

INSERT INTO t_right SELECT number, (number + 1, 'x' || toString(number), tuple(number + 10)) FROM numbers(3);
INSERT INTO t_right SELECT number + 100, if(number = 9, (7, 'y', tuple(70)), (0, '', tuple(0))) FROM numbers(10);

-- The first part has dense Tuple elements, the second one sparse ones.
SELECT arrayDistinct(subcolumns.serializations) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_right' AND column = 't' AND active ORDER BY name;

CREATE TABLE t_left (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_left VALUES (0), (1), (2), (100), (101), (109), (200);

SELECT l.id, r.t FROM t_left AS l INNER JOIN t_right AS r ON l.id = r.id ORDER BY l.id
SETTINGS join_algorithm = 'direct', enable_analyzer = 1, max_threads = 1;

SELECT l.id, r.t FROM t_left AS l LEFT JOIN t_right AS r ON l.id = r.id ORDER BY l.id
SETTINGS join_algorithm = 'direct', enable_analyzer = 1, max_threads = 1;

DROP TABLE t_left;
DROP TABLE t_right;
