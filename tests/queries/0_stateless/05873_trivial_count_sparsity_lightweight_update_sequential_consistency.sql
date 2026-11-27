-- Tags: zookeeper

-- With `select_sequential_consistency`, a query that is a part of a lightweight update reads the parts up to the quorum
-- boundary, clamped by the block numbers that the update allocated. The update allocates block numbers only in
-- the partitions that it affects, and the other partitions are still read. The trivial `count()` rewrite of
-- `optimize_trivial_count_with_sparsity_filter` must count the defaults of the same parts as the row scan reads.

DROP TABLE IF EXISTS t_sparsity_lwu_sequential SYNC;

CREATE TABLE t_sparsity_lwu_sequential (p UInt64, x UInt64, y UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_sparsity_lwu_sequential', 'r1')
PARTITION BY p ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
    ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;

SET enable_lightweight_update = 1, enable_analyzer = 1, enable_parallel_replicas = 0, make_distributed_plan = 0;
SET optimize_trivial_count_query = 1;

INSERT INTO t_sparsity_lwu_sequential VALUES (1, 0, 0);
INSERT INTO t_sparsity_lwu_sequential VALUES (2, 0, 0);
INSERT INTO t_sparsity_lwu_sequential VALUES (3, 0, 0);

-- The update allocates a block number only in the partition 1, and its subquery reads both partitions 1 and 2.
UPDATE t_sparsity_lwu_sequential SET y = (SELECT count() FROM t_sparsity_lwu_sequential WHERE x = 0)
IN PARTITION 1 WHERE p = 1
SETTINGS select_sequential_consistency = 1, optimize_trivial_count_with_sparsity_filter = 0;

UPDATE t_sparsity_lwu_sequential SET y = (SELECT count() FROM t_sparsity_lwu_sequential WHERE x = 0)
IN PARTITION 2 WHERE p = 2
SETTINGS select_sequential_consistency = 1, optimize_trivial_count_with_sparsity_filter = 1;

-- Without `select_sequential_consistency`, only the partitions with the allocated block numbers are read.
UPDATE t_sparsity_lwu_sequential SET x = 1, y = (SELECT count() FROM t_sparsity_lwu_sequential WHERE x = 0)
IN PARTITION 3 WHERE p = 3
SETTINGS select_sequential_consistency = 0, optimize_trivial_count_with_sparsity_filter = 0;

SELECT p, y FROM t_sparsity_lwu_sequential ORDER BY p;

DROP TABLE t_sparsity_lwu_sequential SYNC;
