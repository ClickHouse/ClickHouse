-- The value of `optimize_constant_columns_after_filter` is taken from the context of the query the filter belongs to,
-- so the `SETTINGS` clause of a subquery overrides the one of the outer query.

SELECT 'inner disabled, outer enabled';
SELECT dumpColumnStructure(x) LIKE '%Const%'
FROM
(
    SELECT x FROM values('x UInt64', 1, 2, 1)
    WHERE x = 1
    SETTINGS optimize_constant_columns_after_filter = 0
)
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'inner enabled, outer disabled';
SELECT dumpColumnStructure(x) LIKE '%Const%'
FROM
(
    SELECT x FROM values('x UInt64', 1, 2, 1)
    WHERE x = 1
    SETTINGS optimize_constant_columns_after_filter = 1
)
SETTINGS optimize_constant_columns_after_filter = 0;
