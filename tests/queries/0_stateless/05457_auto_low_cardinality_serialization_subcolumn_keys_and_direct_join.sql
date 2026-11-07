-- Tags: no-old-analyzer

-- A column stored with automatic `LowCardinality` serialization flows through the pipeline as a
-- non-native `ColumnLowCardinality`. It must be accepted:
-- - by `INSERT SELECT` into a plain table whose sorting or partition key uses a subcolumn of it
--   (the subcolumn is extracted in memory before the block is written);
-- - by the `direct` join on a `MergeTree` right table whose parts have different serialization
--   kinds of the projected payload (the lookup blocks are concatenated).

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_auto_lc_src;
DROP TABLE IF EXISTS t_auto_lc_dst_order;
DROP TABLE IF EXISTS t_auto_lc_dst_partition;

CREATE TABLE t_auto_lc_src
(
    id UInt64,
    s String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 100,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_auto_lc_src;

-- High cardinality -> `Default`.
INSERT INTO t_auto_lc_src SELECT number, 'a_' || toString(number) FROM numbers(300);
-- Low cardinality -> `LowCardinality`.
INSERT INTO t_auto_lc_src SELECT number, 'b_' || toString(number % 10) FROM numbers(300, 300);

SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_src' AND active AND column = 's'
ORDER BY serialization_kind;

SELECT 'sorting key on a subcolumn';
CREATE TABLE t_auto_lc_dst_order (s String) ENGINE = MergeTree ORDER BY s.size;
INSERT INTO t_auto_lc_dst_order SELECT s FROM t_auto_lc_src WHERE id >= 300 SETTINGS max_threads = 1;
INSERT INTO t_auto_lc_dst_order SELECT s FROM t_auto_lc_src SETTINGS max_threads = 1;
SELECT count(), uniqExact(s), sum(length(s)) FROM t_auto_lc_dst_order;

SELECT 'partition key on a subcolumn';
CREATE TABLE t_auto_lc_dst_partition (s String) ENGINE = MergeTree PARTITION BY s.size ORDER BY tuple();
INSERT INTO t_auto_lc_dst_partition SELECT s FROM t_auto_lc_src WHERE id >= 300 SETTINGS max_threads = 1;
INSERT INTO t_auto_lc_dst_partition SELECT s FROM t_auto_lc_src SETTINGS max_threads = 1;
SELECT _partition_id, count() FROM t_auto_lc_dst_partition GROUP BY _partition_id ORDER BY _partition_id;

SELECT 'direct join';
SELECT count(), uniqExact(r.s), max(r.s), min(r.s)
FROM t_auto_lc_src AS l INNER ALL JOIN t_auto_lc_src AS r ON l.id = r.id
SETTINGS join_algorithm = 'direct', max_threads = 1, max_block_size = 100;

SELECT count(), uniqExact(r.s), max(r.s), min(r.s)
FROM (SELECT number AS id FROM numbers(600)) AS l INNER ALL JOIN t_auto_lc_src AS r ON l.id = r.id
SETTINGS join_algorithm = 'direct', max_threads = 1, max_block_size = 100;

DROP TABLE t_auto_lc_dst_partition;
DROP TABLE t_auto_lc_dst_order;
DROP TABLE t_auto_lc_src;
