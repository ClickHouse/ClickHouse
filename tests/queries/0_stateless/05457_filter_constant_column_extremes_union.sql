-- Extremes must take into account all chunks when a column is constant only within a chunk:
-- `FilterTransform` replaces `x` with `ColumnConst` after `WHERE x = ...`, and its value differs between `UNION ALL` branches.

SET max_threads = 1, max_streams_for_union_step = 1, optimize_move_to_prewhere = 0;

SELECT x FROM
(
    SELECT x FROM values('x UInt64', 1, 1, 0) WHERE x = 1
    UNION ALL
    SELECT x FROM values('x UInt64', 2, 2, 0) WHERE x = 2
    UNION ALL
    SELECT x FROM values('x UInt64', 0, 3) WHERE x = 0
)
ORDER BY x
SETTINGS extremes = 1, optimize_constant_columns_after_filter = 1
FORMAT TSV;

SELECT x FROM
(
    SELECT x FROM values('x UInt64', 1, 1, 0) WHERE x = 1
    UNION ALL
    SELECT x FROM values('x UInt64', 2, 2, 0) WHERE x = 2
    UNION ALL
    SELECT x FROM values('x UInt64', 0, 3) WHERE x = 0
)
ORDER BY x
SETTINGS extremes = 1, optimize_constant_columns_after_filter = 0
FORMAT TSV;

-- Without sorting, the constant chunks reach `ExtremesTransform` one by one.
SELECT x FROM
(
    SELECT x FROM values('x UInt64', 1, 1, 0) WHERE x = 1
    UNION ALL
    SELECT x FROM values('x UInt64', 2, 2, 0) WHERE x = 2
)
SETTINGS extremes = 1, optimize_constant_columns_after_filter = 1, output_format_write_statistics = 0
FORMAT JSONCompact;

SELECT x FROM
(
    SELECT x FROM values('x UInt64', 2, 2, 0) WHERE x = 2
    UNION ALL
    SELECT x FROM values('x UInt64', 1, 1, 0) WHERE x = 1
)
SETTINGS extremes = 1, optimize_constant_columns_after_filter = 1, output_format_write_statistics = 0
FORMAT JSONCompact;

-- A constant from the query itself keeps working.
SELECT 1 AS c, x FROM values('x UInt64', 5, 7) WHERE x = 5 SETTINGS extremes = 1 FORMAT TSV;
