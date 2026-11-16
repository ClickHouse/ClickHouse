-- Tags: zookeeper

-- `max_memory_ratio_to_activate_adaptive_write_buffer` depends on the memory limit of the server, and adaptive write
-- buffers change the boundaries of compressed blocks, so the offsets in the marks. Replicas compare the checksums
-- of merged and mutated parts, which include the uncompressed marks, so for a replicated table the ratio must not
-- affect merges and mutations: their parts must be the same as with the ratio set to 0. Inserts and non-replicated
-- merges keep using it.

DROP TABLE IF EXISTS t_ratio_replicated;
DROP TABLE IF EXISTS t_no_ratio_replicated;
DROP TABLE IF EXISTS t_ratio_plain;

CREATE TABLE t_ratio_replicated (a UInt64, b UInt64, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_ratio_replicated', 'r1') ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 10485760, min_columns_to_activate_adaptive_write_buffer = 0,
    max_memory_ratio_to_activate_adaptive_write_buffer = 0.000000001, adaptive_write_buffer_initial_size = 4096,
    max_compress_block_size = 1048576, min_compress_block_size = 65536, enable_vertical_merge_algorithm = 0;

CREATE TABLE t_no_ratio_replicated (a UInt64, b UInt64, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_no_ratio_replicated', 'r1') ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 10485760, min_columns_to_activate_adaptive_write_buffer = 0,
    max_memory_ratio_to_activate_adaptive_write_buffer = 0, adaptive_write_buffer_initial_size = 4096,
    max_compress_block_size = 1048576, min_compress_block_size = 65536, enable_vertical_merge_algorithm = 0;

CREATE TABLE t_ratio_plain (a UInt64, b UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 10485760, min_columns_to_activate_adaptive_write_buffer = 0,
    max_memory_ratio_to_activate_adaptive_write_buffer = 0.000000001, adaptive_write_buffer_initial_size = 4096,
    max_compress_block_size = 1048576, min_compress_block_size = 65536, enable_vertical_merge_algorithm = 0;

SYSTEM STOP MERGES t_ratio_replicated;
SYSTEM STOP MERGES t_no_ratio_replicated;
SYSTEM STOP MERGES t_ratio_plain;

INSERT INTO t_ratio_replicated SELECT number, number * 3, repeat('x', number % 50) FROM numbers(20000);
INSERT INTO t_ratio_replicated SELECT number + 20000, number * 5, repeat('y', number % 30) FROM numbers(20000);
INSERT INTO t_no_ratio_replicated SELECT number, number * 3, repeat('x', number % 50) FROM numbers(20000);
INSERT INTO t_no_ratio_replicated SELECT number + 20000, number * 5, repeat('y', number % 30) FROM numbers(20000);
INSERT INTO t_ratio_plain SELECT number, number * 3, repeat('x', number % 50) FROM numbers(20000);
INSERT INTO t_ratio_plain SELECT number + 20000, number * 5, repeat('y', number % 30) FROM numbers(20000);

-- Inserted parts use adaptive buffers by the ratio, so their marks differ.
SELECT 'insert, replicated vs no ratio',
    (SELECT groupArray(tuple(*)) FROM (SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_ratio_replicated, with_marks = true) ORDER BY part_name, mark_number))
    = (SELECT groupArray(tuple(*)) FROM (SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_no_ratio_replicated, with_marks = true) ORDER BY part_name, mark_number));

SYSTEM START MERGES t_ratio_replicated;
SYSTEM START MERGES t_no_ratio_replicated;
SYSTEM START MERGES t_ratio_plain;

OPTIMIZE TABLE t_ratio_replicated FINAL;
OPTIMIZE TABLE t_no_ratio_replicated FINAL;
OPTIMIZE TABLE t_ratio_plain FINAL;

SELECT 'merge, replicated vs no ratio',
    (SELECT groupArray(tuple(*)) FROM (SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_ratio_replicated, with_marks = true) ORDER BY mark_number))
    = (SELECT groupArray(tuple(*)) FROM (SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_no_ratio_replicated, with_marks = true) ORDER BY mark_number));

SELECT 'merge, plain vs no ratio',
    (SELECT groupArray(tuple(*)) FROM (SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_ratio_plain, with_marks = true) ORDER BY mark_number))
    = (SELECT groupArray(tuple(*)) FROM (SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_no_ratio_replicated, with_marks = true) ORDER BY mark_number));

-- A mutation of one column writes only that column.
ALTER TABLE t_ratio_replicated UPDATE b = b + 1 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_no_ratio_replicated UPDATE b = b + 1 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_ratio_plain UPDATE b = b + 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'mutation, replicated vs no ratio',
    (SELECT groupArray(tuple(*)) FROM (SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_ratio_replicated, with_marks = true) ORDER BY mark_number))
    = (SELECT groupArray(tuple(*)) FROM (SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_no_ratio_replicated, with_marks = true) ORDER BY mark_number));

SELECT 'mutation, plain vs no ratio',
    (SELECT groupArray(tuple(*)) FROM (SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_ratio_plain, with_marks = true) ORDER BY mark_number))
    = (SELECT groupArray(tuple(*)) FROM (SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_no_ratio_replicated, with_marks = true) ORDER BY mark_number));

SELECT count(), sum(a), sum(b), sum(length(s)) FROM t_ratio_replicated;

DROP TABLE t_ratio_replicated;
DROP TABLE t_no_ratio_replicated;
DROP TABLE t_ratio_plain;
