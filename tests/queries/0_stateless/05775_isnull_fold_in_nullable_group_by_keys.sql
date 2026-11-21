-- https://github.com/ClickHouse/ClickHouse/issues/133377
-- With group_by_use_nulls = 1, a GROUP BY key whose subexpression isNull(x) was folded to a
-- constant while x was not yet Nullable must still match the projection after the keys are
-- converted to Nullable: the emitted rows must carry the key column values, not a
-- re-evaluation of the CASE over the NULL-filled keys.

DROP TABLE IF EXISTS t_isnull_rollup;
CREATE TABLE t_isnull_rollup (id Int32, x Nullable(Int32)) ENGINE = Memory;
INSERT INTO t_isnull_rollup VALUES (1, 1), (2, NULL), (3, 2);

SET group_by_use_nulls = 1;

SELECT e, x, count() FROM
(
    SELECT CASE WHEN x IS NULL THEN 'n/a' ELSE toString(x) END AS e, x FROM t_isnull_rollup
)
GROUP BY ROLLUP(e, x)
ORDER BY e NULLS LAST, x NULLS FIRST;

SELECT e, x, count() FROM
(
    SELECT CASE WHEN x IS NULL THEN 'n/a' ELSE toString(x) END AS e, x FROM t_isnull_rollup
)
GROUP BY GROUPING SETS ((e, x), (e), ())
ORDER BY e NULLS LAST, x NULLS FIRST;

SET group_by_use_nulls = 0;
DROP TABLE t_isnull_rollup;
