-- A `DEFAULT` expression that reads a subcolumn of an omitted column takes it from the value that the column gets.
-- The column is referred to by its name, which a parameter of a lambda around the reference may have as well:
-- the quoted `p.size0` is the subcolumn of the table, not a subcolumn of the parameter `p`, and it stays so.
-- The results with the column omitted and supplied are the same.

DROP TABLE IF EXISTS t_shadow_lambda_capture;
CREATE TABLE t_shadow_lambda_capture
(
    id UInt8,
    p Array(UInt64) DEFAULT [7],
    d UInt64 DEFAULT arraySum(arrayMap(p -> `p.size0`, [[1, 2, 3]])),
    e UInt64 DEFAULT arraySum(arrayMap(p -> `p.size0` + length(p), [[1, 2, 3]])),
    f UInt64 DEFAULT arraySum(arrayMap(p -> arraySum(arrayMap(q -> q + `p.size0` + length(p), [10])), [[1, 2, 3]])),
    g UInt64 DEFAULT arraySum(arrayMap(p -> arraySum(arrayMap(p -> p + `p.size0`, [10])), [[1, 2, 3]])),
    h UInt64 DEFAULT arraySum(arrayMap((p, q) -> `p.size0` + p.1 + q, [(100, 'a')], [1000]))
)
ENGINE = MergeTree ORDER BY id;

SELECT '-- the column `p` is omitted';
INSERT INTO t_shadow_lambda_capture (id) VALUES (1);
SELECT '-- the column `p` is supplied';
INSERT INTO t_shadow_lambda_capture (id, p) VALUES (2, [7]);

SELECT id, p, d, e, f, g, h FROM t_shadow_lambda_capture ORDER BY id;

DROP TABLE t_shadow_lambda_capture;
