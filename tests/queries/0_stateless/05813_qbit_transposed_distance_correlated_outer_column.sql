-- https://github.com/ClickHouse/ClickHouse/issues/122344
-- A transposed distance over a QBit column of the outer query inside a correlated subquery must not be
-- rewritten into bit-plane subcolumns: the subquery receives the whole column (`NOT_FOUND_COLUMN_IN_BLOCK`).

SET allow_experimental_correlated_subqueries = 1;
SET optimize_qbit_distance_function_reads = 1;

DROP TABLE IF EXISTS t_qbit;
CREATE TABLE t_qbit (id UInt32, vec QBit(Float32, 4), nvec Nullable(QBit(Float32, 4)), q QBit(Int8, 4)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_qbit VALUES (1, [1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 3, 4]), (2, [5, 6, 7, 8], NULL, [50, 60, 70, 80]), (3, [1, 2, 3, 5], [1, 2, 3, 5], [1, 2, 3, 5]);

SELECT 'exists';
SELECT count() FROM t_qbit AS o WHERE EXISTS (SELECT 1 FROM t_qbit AS i WHERE i.id = o.id
    AND L2DistanceTransposed(o.vec, [1., 2., 3., 4.], 32) < 2
    AND cosineDistanceTransposed(o.vec, [1., 2., 3., 4.], 32) < 0.01
    AND dotProductTransposed(o.vec, [1., 2., 3., 4.], 32) < 50);

SELECT 'scalar';
SELECT o.id, (SELECT min(L2DistanceTransposed(o.vec, [1., 2., 3., 4.], 32) + i.id) FROM t_qbit AS i) FROM t_qbit AS o ORDER BY o.id;

SELECT 'union';
SELECT count() FROM t_qbit AS o WHERE EXISTS (
    SELECT 1 FROM t_qbit AS i WHERE i.id = o.id AND L2DistanceTransposed(o.vec, [1., 2., 3., 4.], 32) < 2
    UNION ALL
    SELECT 1 FROM t_qbit AS i WHERE i.id = o.id AND cosineDistanceTransposed(o.vec, [1., 2., 3., 4.], 32) < 0.01);

SELECT 'two levels down';
SELECT count() FROM t_qbit AS o WHERE EXISTS (SELECT 1 FROM (SELECT id FROM t_qbit AS i WHERE i.id = o.id AND L2DistanceTransposed(o.vec, [1., 2., 3., 4.], 32) < 2));

SELECT 'in and not in';
SELECT id FROM t_qbit WHERE L2DistanceTransposed(vec, [1., 2., 3., 4.], 32) IN (SELECT toFloat64(0)) ORDER BY id SETTINGS rewrite_in_to_join = 1;
SELECT id FROM t_qbit WHERE L2DistanceTransposed(vec, [1., 2., 3., 4.], 32) NOT IN (SELECT toFloat64(0)) ORDER BY id SETTINGS rewrite_in_to_join = 1;

SELECT 'nullable and quantized';
SELECT count() FROM t_qbit AS o WHERE EXISTS (SELECT 1 FROM t_qbit AS i WHERE i.id = o.id
    AND L2DistanceTransposed(o.nvec, [1., 2., 3., 4.], 32) < 2
    AND L2DistanceTransposedQuantized(o.q, [1, 2, 3, 4]::Array(Int8), 8) < 0.5);

SELECT 'outer and correlated use';
SELECT o.id, L2DistanceTransposed(o.vec, [1., 2., 3., 4.], 32) FROM t_qbit AS o
WHERE EXISTS (SELECT 1 FROM t_qbit AS i WHERE i.id = o.id AND L2DistanceTransposed(o.vec, [1., 2., 3., 4.], 32) < 2) ORDER BY o.id;
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT o.id, L2DistanceTransposed(o.vec, [1., 2., 3., 4.], 32) FROM t_qbit AS o
    WHERE EXISTS (SELECT 1 FROM t_qbit AS i WHERE i.id = o.id AND L2DistanceTransposed(o.vec, [1., 2., 3., 4.], 32) < 2) ORDER BY o.id)
WHERE explain ILIKE '%column_name: vec.1,%';
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT o.id FROM t_qbit AS o
    WHERE EXISTS (SELECT 1 FROM t_qbit AS i WHERE i.id = o.id AND L2DistanceTransposed(o.vec, [1., 2., 3., 4.], 32) < 2)
    ORDER BY L2DistanceTransposed(o.vec, [1., 2., 3., 4.], 32), o.id)
WHERE explain ILIKE '%column_name: vec.1,%';

SELECT 'only the inner column is rewritten';
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT count() FROM t_qbit AS o WHERE EXISTS (SELECT 1 FROM t_qbit AS i WHERE i.id = o.id
        AND L2DistanceTransposed(i.vec, [1., 2., 3., 4.], 32) < 2 AND L2DistanceTransposed(o.vec, [1., 2., 3., 4.], 32) < 2))
WHERE explain ILIKE '%column_name: vec.1,%';

DROP TABLE t_qbit;
