-- The column default, which `basic` statistics count exactly, keeps its count in the estimate of an `IN` set that
-- holds it after other values: `with_default IN (1, 2, 0)` passes half the rows, so `filter_col = 5` is read first.

DROP TABLE IF EXISTS t_counted_default;

-- Compact parts: PREWHERE conditions are then ordered by their estimated row counts alone.
CREATE TABLE t_counted_default (with_default UInt64, filter_col UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 1000000000000, min_rows_for_wide_part = 1000000000000;

INSERT INTO t_counted_default SELECT if(number % 2 = 0, 0, number), number % 100 FROM numbers(10000);
INSERT INTO t_counted_default SELECT if(number % 2 = 0, 0, number + 10000), number % 100 FROM numbers(10000);
OPTIMIZE TABLE t_counted_default FINAL;

SELECT position(line, 'filter_col') < position(line, 'with_default') AS filter_col_first
FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM t_counted_default WHERE with_default IN (1, 2, 0) AND filter_col = 5
        SETTINGS use_statistics = 1, optimize_move_to_prewhere = 1, allow_reorder_prewhere_conditions = 1,
            min_marks_per_part_to_load_statistics = 0, explain_query_plan_default = 'legacy'
    )
    WHERE explain ILIKE '%Prewhere filter column%'
);

DROP TABLE t_counted_default;
