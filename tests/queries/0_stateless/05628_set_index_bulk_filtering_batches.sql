-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN indexes = 1 gains a per-node Granules block.

-- Bulk filtering of a set index evaluates the condition in batches of about 65k stored values.
-- 600 granules of 1000 rows with disjoint values: granules with g % 10 = 3 hold 1000 distinct values,
-- which saturates set(500) so they store nothing and are always kept; the other 540 store 500 values
-- each, so the part spans five batches. The values searched lie in granule 0, in the granule that
-- closes the first batch (145), in the first granule of the next one (146) and in the last granule.

SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_set_batches;
CREATE TABLE t_set_batches (g UInt64, v UInt64, INDEX idx v TYPE set(500) GRANULARITY 1)
ENGINE = MergeTree ORDER BY g
SETTINGS index_granularity = 1000, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_set_batches SELECT intDiv(number, 1000) AS g, if(g % 10 = 3, number, g * 1000 + number % 500)
FROM numbers(600000);
OPTIMIZE TABLE t_set_batches FINAL;

SELECT count() FROM t_set_batches WHERE v IN (7, 145007, 146007, 599007, 999999999) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_set_batches WHERE v IN (7, 145007, 146007, 599007, 999999999) SETTINGS secondary_indices_enable_bulk_filtering = 1, use_query_condition_cache = 0;
SELECT count() FROM t_set_batches WHERE v IN (7, 145007, 146007, 599007, 999999999) SETTINGS secondary_indices_enable_bulk_filtering = 0, use_query_condition_cache = 0;

-- 4 granules with a match + 60 saturated granules.
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set_batches WHERE v IN (7, 145007, 146007, 599007, 999999999)
    SETTINGS secondary_indices_enable_bulk_filtering = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0)
WHERE explain LIKE '%Granules: 64/600%';

DROP TABLE t_set_batches;
