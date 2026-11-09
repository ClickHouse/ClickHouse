-- `optimize_constant_columns_after_filter` can turn a sort key into a `ColumnConst` at runtime.
-- The threshold filter of `PartialSortingTransform` must handle it: in a multi-column comparison
-- `ColumnConst::compareColumn` ignores `row_indexes`, and the same key can be constant in one chunk and full in another.

DROP TABLE IF EXISTS t_filter_const_sort;
DROP TABLE IF EXISTS t_filter_const_sort_mixed;

CREATE TABLE t_filter_const_sort (x UInt64, y UInt64) ENGINE = Memory;
INSERT INTO t_filter_const_sort SELECT 1, 2 FROM numbers(2000);
INSERT INTO t_filter_const_sort SELECT 1, 1 + number % 2 FROM numbers(4000);

SET max_threads = 1, optimize_move_to_prewhere = 0, optimize_constant_columns_after_filter = 1;

-- Multiple sort keys, one of them constant.
SELECT sum(y), count() FROM (SELECT x, y FROM t_filter_const_sort WHERE x = 1 ORDER BY y, x LIMIT 1500);
SELECT sum(y), count() FROM (SELECT x, y FROM t_filter_const_sort WHERE x = 1 ORDER BY y, x LIMIT 1500)
SETTINGS optimize_constant_columns_after_filter = 0;

-- The sort key is full in some chunks and constant in others.
CREATE TABLE t_filter_const_sort_mixed (x UInt64) ENGINE = Memory;
INSERT INTO t_filter_const_sort_mixed SELECT number % 2 FROM numbers(8000);

SELECT sum(x), count()
FROM
(
    SELECT x FROM
    (
        SELECT number AS x FROM numbers(8000)
        UNION ALL
        SELECT x FROM t_filter_const_sort_mixed WHERE x = 1
    )
    ORDER BY x LIMIT 1500
)
SETTINGS max_block_size = 2000, max_streams_for_union_step = 1;

DROP TABLE t_filter_const_sort;
DROP TABLE t_filter_const_sort_mixed;
