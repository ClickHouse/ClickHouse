-- A `DEFAULT` expression that reads a subcolumn of an omitted column takes it from the value that the column gets.
-- Only the free references are meant: a parameter of a lambda with the same name stays the parameter.

DROP TABLE IF EXISTS t_shadow_lambda;
CREATE TABLE t_shadow_lambda
(
    id UInt8,
    p Tuple(x UInt64) DEFAULT tuple(toUInt64(7)),
    d UInt64 DEFAULT p.x + arraySum(arrayMap(`p.x` -> `p.x`, [1])),
    e UInt64 DEFAULT arraySum(arrayMap(q -> q + p.x, [1, 2])),
    f UInt64 DEFAULT p.x + arraySum(arrayMap((`p.x`, q) -> `p.x` * q, [1, 2], [10, 100]))
)
ENGINE = MergeTree ORDER BY id;

SELECT '-- the column `p` is omitted';
INSERT INTO t_shadow_lambda (id) VALUES (1);
SELECT '-- the column `p` is supplied';
INSERT INTO t_shadow_lambda (id, p) VALUES (2, tuple(40));

SELECT id, p, d, e, f FROM t_shadow_lambda ORDER BY id;

DROP TABLE t_shadow_lambda;
