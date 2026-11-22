-- A SHARED typed path of JSON stays in the default layout in a part, while the other typed paths may become Sparse.
-- The SHARED path is read as a subcolumn, as the whole column, and in a sub-object next to a Sparse typed path.

DROP TABLE IF EXISTS t_shared_sparse_wide;
DROP TABLE IF EXISTS t_shared_sparse_compact;

CREATE TABLE t_shared_sparse_wide (id UInt64, j JSON(SHARED a UInt64, b UInt64, SHARED p.x UInt64, p.y UInt64, max_dynamic_paths = 0))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.5, serialization_info_version = 'with_subcolumns';

CREATE TABLE t_shared_sparse_compact AS t_shared_sparse_wide
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, ratio_of_defaults_for_sparse_serialization = 0.5, serialization_info_version = 'with_subcolumns';

INSERT INTO t_shared_sparse_wide SELECT number, if(number = 0, '{"a":1,"b":2,"p":{"x":3,"y":4}}', '{}') FROM numbers(100);
INSERT INTO t_shared_sparse_compact SELECT * FROM t_shared_sparse_wide;

SELECT sum(j.a), sum(j.b), sum(j.p.x), sum(j.p.y) FROM t_shared_sparse_wide;
SELECT j.a, j.b, j.^p, j.@p, j FROM t_shared_sparse_wide WHERE id = 0;
SELECT sum(j.a), sum(j.b), sum(j.p.x), sum(j.p.y) FROM t_shared_sparse_compact;
SELECT j.a, j.b, j.^p, j.@p, j FROM t_shared_sparse_compact WHERE id = 0;

INSERT INTO t_shared_sparse_wide SELECT * FROM t_shared_sparse_compact;
OPTIMIZE TABLE t_shared_sparse_wide FINAL;
SELECT sum(j.a), sum(j.b), sum(j.p.x), sum(j.p.y), countIf(toString(j.^p) = '{"x":3,"y":4}') FROM t_shared_sparse_wide;

DROP TABLE t_shared_sparse_wide;
DROP TABLE t_shared_sparse_compact;
