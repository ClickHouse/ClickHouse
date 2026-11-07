-- Tags: no-random-settings, no-random-merge-tree-settings

-- `compact_parts_max_bytes_to_buffer` must count the states of `AggregateFunction` columns: `IColumn::byteSize`
-- of `ColumnAggregateFunction` does not include them. Every state here is about 8 KiB, so a stripe limited to
-- 64 KiB holds at most 8 granules of 1 row (it held all 128 granules when the states were not counted).
-- A granule starts a new stripe when its mark of `id` is after the mark of `s` of the previous granule.

DROP TABLE IF EXISTS t_compact_stripes_agg;

CREATE TABLE t_compact_stripes_agg (id UInt64, s AggregateFunction(groupArray, UInt64), z UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = '1G', merge_max_block_size = 1,
    compact_parts_max_granules_to_buffer = 128, compact_parts_max_bytes_to_buffer = 65536, compress_per_column_in_compact_parts = 1;

SYSTEM STOP MERGES t_compact_stripes_agg;

INSERT INTO t_compact_stripes_agg SELECT number, arrayReduce('groupArrayState', range(number, number + 1024)), number FROM numbers(128);
INSERT INTO t_compact_stripes_agg SELECT number, arrayReduce('groupArrayState', range(number, number + 1024)), number FROM numbers(128, 128);

SELECT 'insert';
SELECT max(granules) BETWEEN 2 AND 8, count() >= 2 * 128 / 8
FROM
(
    SELECT count() AS granules
    FROM
    (
        SELECT part_name, sum(stripe_start) OVER (PARTITION BY part_name ORDER BY mark_number) AS stripe
        FROM
        (
            SELECT part_name, mark_number, id.mark.1 > lagInFrame(s.mark.1, 1, 0) OVER (PARTITION BY part_name ORDER BY mark_number) AS stripe_start
            FROM mergeTreeIndex(currentDatabase(), t_compact_stripes_agg, with_marks = true)
            WHERE rows_in_granule > 0
        )
    )
    GROUP BY part_name, stripe
);

SYSTEM START MERGES t_compact_stripes_agg;
OPTIMIZE TABLE t_compact_stripes_agg FINAL;

SELECT 'merge';
SELECT max(granules) BETWEEN 2 AND 8, count() >= 256 / 8
FROM
(
    SELECT count() AS granules
    FROM
    (
        SELECT part_name, sum(stripe_start) OVER (PARTITION BY part_name ORDER BY mark_number) AS stripe
        FROM
        (
            SELECT part_name, mark_number, id.mark.1 > lagInFrame(s.mark.1, 1, 0) OVER (PARTITION BY part_name ORDER BY mark_number) AS stripe_start
            FROM mergeTreeIndex(currentDatabase(), t_compact_stripes_agg, with_marks = true)
            WHERE rows_in_granule > 0
        )
    )
    GROUP BY part_name, stripe
);

SELECT count(), sum(id), sum(z), sum(length(finalizeAggregation(s))) FROM t_compact_stripes_agg;
SELECT groupArrayMerge(s)[1024] FROM t_compact_stripes_agg WHERE id = 200;

DROP TABLE t_compact_stripes_agg;
