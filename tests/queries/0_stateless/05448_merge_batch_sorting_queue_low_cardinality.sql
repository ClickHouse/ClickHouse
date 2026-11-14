-- The result of a merge must not depend on `merge_use_batch_sorting_queue`, including for
-- `LowCardinality` columns, for which `byteSizeAt` only estimates the growth of the merged block,
-- and for wide values that cut a batch on `merge_max_block_size_bytes`.

DROP TABLE IF EXISTS t_merge_batch_lc_off;
DROP TABLE IF EXISTS t_merge_batch_lc_on;

CREATE TABLE t_merge_batch_lc_off
(
    k UInt64,
    s LowCardinality(String),
    m Map(LowCardinality(String), LowCardinality(String)),
    body String
)
ENGINE = MergeTree ORDER BY k
SETTINGS merge_use_batch_sorting_queue = 0, merge_max_block_size_bytes = 65536;

CREATE TABLE t_merge_batch_lc_on AS t_merge_batch_lc_off
ENGINE = MergeTree ORDER BY k
SETTINGS merge_use_batch_sorting_queue = 1, merge_max_block_size_bytes = 65536;

SYSTEM STOP MERGES t_merge_batch_lc_off;
SYSTEM STOP MERGES t_merge_batch_lc_on;

-- Three fully interleaved parts (single-row runs between cursors) and one disjoint part
-- (long runs). Every part has runs of three 200 KB values, larger than the byte limit.
INSERT INTO t_merge_batch_lc_off SELECT number * 3, 'v' || toString(number % 100), map('a', 'x' || toString(number % 1000), 'b', 'y' || toString(number % 10)), repeat('w', if(number % 1000 < 3, 200000, 1)) FROM numbers(3000);
INSERT INTO t_merge_batch_lc_off SELECT number * 3 + 1, 'v' || toString(number % 100), map('a', 'x' || toString(number % 1000), 'b', 'y' || toString(number % 10)), repeat('w', if(number % 1000 < 3, 200000, 1)) FROM numbers(3000);
INSERT INTO t_merge_batch_lc_off SELECT number * 3 + 2, 'v' || toString(number % 100), map('a', 'x' || toString(number % 1000), 'b', 'y' || toString(number % 10)), repeat('w', if(number % 1000 < 3, 200000, 1)) FROM numbers(3000);
INSERT INTO t_merge_batch_lc_off SELECT 10000 + number, 'v' || toString(number % 100), map('a', 'x' || toString(number % 1000), 'b', 'y' || toString(number % 10)), repeat('w', if(number % 1000 < 3, 200000, 1)) FROM numbers(3000);

-- Both tables merge identical parts.
ALTER TABLE t_merge_batch_lc_on ATTACH PARTITION ID 'all' FROM t_merge_batch_lc_off;

SYSTEM START MERGES t_merge_batch_lc_off;
SYSTEM START MERGES t_merge_batch_lc_on;

OPTIMIZE TABLE t_merge_batch_lc_off FINAL;
OPTIMIZE TABLE t_merge_batch_lc_on FINAL;

SELECT (SELECT count() FROM t_merge_batch_lc_off), (SELECT count() FROM t_merge_batch_lc_on);
SELECT count() FROM (SELECT * FROM t_merge_batch_lc_off EXCEPT SELECT * FROM t_merge_batch_lc_on);
SELECT count() FROM (SELECT * FROM t_merge_batch_lc_on EXCEPT SELECT * FROM t_merge_batch_lc_off);

DROP TABLE t_merge_batch_lc_off;
DROP TABLE t_merge_batch_lc_on;
