-- With `do_not_merge_across_partitions_select_final`, lazy FINAL must keep the newest row of a key in every
-- partition, like regular FINAL.

SET query_plan_optimize_lazy_final = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_lazy_final_partitions;
CREATE TABLE t_lazy_final_partitions (k UInt64, p UInt8, v UInt8, ver UInt64)
ENGINE = ReplacingMergeTree(ver) PARTITION BY p ORDER BY k
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_lazy_final_partitions;
INSERT INTO t_lazy_final_partitions SELECT number, 1, number = 0, 1 FROM numbers(1000);
INSERT INTO t_lazy_final_partitions SELECT number, 2, number = 0, 2 FROM numbers(1000);

SELECT 'per partition', arraySort(groupArray((p, ver))) FROM t_lazy_final_partitions FINAL WHERE v = 1
SETTINGS do_not_merge_across_partitions_select_final = 1;
SELECT 'per partition, prewhere', arraySort(groupArray((p, ver))) FROM t_lazy_final_partitions FINAL PREWHERE v = 1
SETTINGS do_not_merge_across_partitions_select_final = 1;
SELECT 'across partitions', arraySort(groupArray((p, ver))) FROM t_lazy_final_partitions FINAL WHERE v = 1
SETTINGS do_not_merge_across_partitions_select_final = 0;
SELECT 'across partitions, lazy', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT p, ver FROM t_lazy_final_partitions FINAL WHERE v = 1 SETTINGS do_not_merge_across_partitions_select_final = 0);

DROP TABLE t_lazy_final_partitions;

-- Without a partition key, or with one computed from the primary key, a key is in one partition: lazy FINAL applies.
DROP TABLE IF EXISTS t_lazy_final_by_key;
CREATE TABLE t_lazy_final_by_key (k UInt64, v UInt8, ver UInt64)
ENGINE = ReplacingMergeTree(ver) PARTITION BY intDiv(k, 500) ORDER BY k
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_lazy_final_by_key;
INSERT INTO t_lazy_final_by_key SELECT number, number % 250 = 0, 1 FROM numbers(1000);
INSERT INTO t_lazy_final_by_key SELECT number, number % 250 = 0, 2 FROM numbers(1000);

SELECT 'by key', arraySort(groupArray((k, ver))) FROM t_lazy_final_by_key FINAL WHERE v = 1
SETTINGS do_not_merge_across_partitions_select_final = 1;
SELECT 'by key, lazy', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT k, ver FROM t_lazy_final_by_key FINAL WHERE v = 1 SETTINGS do_not_merge_across_partitions_select_final = 1);

DROP TABLE t_lazy_final_by_key;

DROP TABLE IF EXISTS t_lazy_final_no_partition;
CREATE TABLE t_lazy_final_no_partition (k UInt64, v UInt8, ver UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY k
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_lazy_final_no_partition;
INSERT INTO t_lazy_final_no_partition SELECT number, number = 0, 1 FROM numbers(1000);
INSERT INTO t_lazy_final_no_partition SELECT number, number = 0, 2 FROM numbers(1000);

SELECT 'no partition', arraySort(groupArray((k, ver))) FROM t_lazy_final_no_partition FINAL WHERE v = 1
SETTINGS do_not_merge_across_partitions_select_final = 1;
SELECT 'no partition, lazy', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT k, ver FROM t_lazy_final_no_partition FINAL WHERE v = 1 SETTINGS do_not_merge_across_partitions_select_final = 1);

DROP TABLE t_lazy_final_no_partition;
