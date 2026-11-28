-- `GraphiteMergeTree` rounds `Time` down in the FINAL merge, so the partition minmax bounds describe
-- the input keys, not the keys emitted by FINAL. Partitions with disjoint input time ranges can emit
-- equal times, and then the order depends on the next sorting key column. Reading the partitions
-- sequentially in the order of the raw bounds would return a wrong top row.

SET max_threads = 1, max_final_threads = 1, optimize_on_insert = 0;
SET optimize_read_in_order = 1, optimize_final_limit_pushdown = 1;
SET do_not_merge_across_partitions_select_final = 1;

DROP TABLE IF EXISTS t_graphite_seq;

CREATE TABLE t_graphite_seq (Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
ENGINE = GraphiteMergeTree('graphite_rollup')
PARTITION BY toStartOfHour(Time) ORDER BY (Time, Path);

SYSTEM STOP MERGES t_graphite_seq;

-- both rows are rolled up into the 6000-second bucket that starts at 959996400, but they are in different partitions
INSERT INTO t_graphite_seq VALUES ('sum_z', 960000100, 1, 1);
INSERT INTO t_graphite_seq VALUES ('sum_a', 960001300, 2, 1);

SELECT Path, toUnixTimestamp(Time), Value FROM t_graphite_seq FINAL
ORDER BY Time, Path LIMIT 1 SETTINGS optimize_final_sequential_partitions = 1;

SELECT Path, toUnixTimestamp(Time), Value FROM t_graphite_seq FINAL
ORDER BY Time, Path LIMIT 1 SETTINGS optimize_final_sequential_partitions = 0;

-- the partitions are not concatenated
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT Path, Time, Value FROM t_graphite_seq FINAL
    ORDER BY Time, Path LIMIT 1 SETTINGS optimize_final_sequential_partitions = 1
) WHERE explain LIKE '%Concat%';

DROP TABLE t_graphite_seq;
