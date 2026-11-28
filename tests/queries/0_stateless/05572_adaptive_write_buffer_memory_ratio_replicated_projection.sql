-- Tags: zookeeper

-- Like 05571, but for projections: `MATERIALIZE PROJECTION` and merges write temporary projection parts
-- (`writeTempProjectionPart`), and a single one is installed as the projection of the resulting part by renaming,
-- so it is compared by replicas too. For a replicated table `max_memory_ratio_to_activate_adaptive_write_buffer`
-- must not affect them: the projection parts must be the same as with the ratio set to 0. Projections written on
-- insert and projections of non-replicated tables keep using it.

DROP TABLE IF EXISTS t_proj_ratio_replicated;
DROP TABLE IF EXISTS t_proj_no_ratio_replicated;
DROP TABLE IF EXISTS t_proj_ratio_plain;

CREATE TABLE t_proj_ratio_replicated (a UInt64, b UInt64, s String, PROJECTION p1 (SELECT a, b, s ORDER BY b))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_proj_ratio_replicated', 'r1') ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 10485760, min_columns_to_activate_adaptive_write_buffer = 0,
    max_memory_ratio_to_activate_adaptive_write_buffer = 0.000000001, adaptive_write_buffer_initial_size = 4096,
    max_compress_block_size = 1048576, min_compress_block_size = 65536, enable_vertical_merge_algorithm = 0;

CREATE TABLE t_proj_no_ratio_replicated (a UInt64, b UInt64, s String, PROJECTION p1 (SELECT a, b, s ORDER BY b))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_proj_no_ratio_replicated', 'r1') ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 10485760, min_columns_to_activate_adaptive_write_buffer = 0,
    max_memory_ratio_to_activate_adaptive_write_buffer = 0, adaptive_write_buffer_initial_size = 4096,
    max_compress_block_size = 1048576, min_compress_block_size = 65536, enable_vertical_merge_algorithm = 0;

CREATE TABLE t_proj_ratio_plain (a UInt64, b UInt64, s String, PROJECTION p1 (SELECT a, b, s ORDER BY b))
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 10485760, min_columns_to_activate_adaptive_write_buffer = 0,
    max_memory_ratio_to_activate_adaptive_write_buffer = 0.000000001, adaptive_write_buffer_initial_size = 4096,
    max_compress_block_size = 1048576, min_compress_block_size = 65536, enable_vertical_merge_algorithm = 0;

SYSTEM STOP MERGES t_proj_ratio_replicated;
SYSTEM STOP MERGES t_proj_no_ratio_replicated;
SYSTEM STOP MERGES t_proj_ratio_plain;

INSERT INTO t_proj_ratio_replicated SELECT number, number * 3, repeat('x', number % 50) FROM numbers(20000);
INSERT INTO t_proj_ratio_replicated SELECT number + 20000, number * 5, repeat('y', number % 30) FROM numbers(20000);
INSERT INTO t_proj_no_ratio_replicated SELECT number, number * 3, repeat('x', number % 50) FROM numbers(20000);
INSERT INTO t_proj_no_ratio_replicated SELECT number + 20000, number * 5, repeat('y', number % 30) FROM numbers(20000);
INSERT INTO t_proj_ratio_plain SELECT number, number * 3, repeat('x', number % 50) FROM numbers(20000);
INSERT INTO t_proj_ratio_plain SELECT number + 20000, number * 5, repeat('y', number % 30) FROM numbers(20000);

-- The hashes of the uncompressed content cover the marks (which depend on the buffers), but not the compressed bytes.
-- Projections written on insert use adaptive buffers by the ratio, so they differ.
SELECT 'insert, replicated vs no ratio',
    (SELECT arraySort(groupArray(tuple(hash_of_uncompressed_files, uncompressed_hash_of_compressed_files))) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_proj_ratio_replicated' AND name = 'p1' AND active)
    = (SELECT arraySort(groupArray(tuple(hash_of_uncompressed_files, uncompressed_hash_of_compressed_files))) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_proj_no_ratio_replicated' AND name = 'p1' AND active);

-- `SYSTEM STOP MERGES` also blocks mutations, so merges are started before `MATERIALIZE PROJECTION`. Merge to one
-- part first, so that background merges do not make the sets of parts differ between the tables.
SYSTEM START MERGES t_proj_ratio_replicated;
SYSTEM START MERGES t_proj_no_ratio_replicated;
SYSTEM START MERGES t_proj_ratio_plain;

OPTIMIZE TABLE t_proj_ratio_replicated FINAL;
OPTIMIZE TABLE t_proj_no_ratio_replicated FINAL;
OPTIMIZE TABLE t_proj_ratio_plain FINAL;

SELECT 'merge, replicated vs no ratio',
    (SELECT arraySort(groupArray(tuple(name, hash_of_uncompressed_files, uncompressed_hash_of_compressed_files))) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_proj_ratio_replicated' AND active)
    = (SELECT arraySort(groupArray(tuple(name, hash_of_uncompressed_files, uncompressed_hash_of_compressed_files))) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_proj_no_ratio_replicated' AND active);

SELECT 'merge, plain vs no ratio',
    (SELECT arraySort(groupArray(tuple(name, hash_of_uncompressed_files, uncompressed_hash_of_compressed_files))) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_proj_ratio_plain' AND active)
    = (SELECT arraySort(groupArray(tuple(name, hash_of_uncompressed_files, uncompressed_hash_of_compressed_files))) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_proj_no_ratio_replicated' AND active);

ALTER TABLE t_proj_ratio_replicated ADD PROJECTION p2 (SELECT a, b ORDER BY b, a);
ALTER TABLE t_proj_no_ratio_replicated ADD PROJECTION p2 (SELECT a, b ORDER BY b, a);
ALTER TABLE t_proj_ratio_plain ADD PROJECTION p2 (SELECT a, b ORDER BY b, a);

ALTER TABLE t_proj_ratio_replicated MATERIALIZE PROJECTION p2 SETTINGS mutations_sync = 2;
ALTER TABLE t_proj_no_ratio_replicated MATERIALIZE PROJECTION p2 SETTINGS mutations_sync = 2;
ALTER TABLE t_proj_ratio_plain MATERIALIZE PROJECTION p2 SETTINGS mutations_sync = 2;

SELECT 'materialize, replicated vs no ratio',
    (SELECT arraySort(groupArray(tuple(hash_of_uncompressed_files, uncompressed_hash_of_compressed_files))) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_proj_ratio_replicated' AND name = 'p2' AND active)
    = (SELECT arraySort(groupArray(tuple(hash_of_uncompressed_files, uncompressed_hash_of_compressed_files))) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_proj_no_ratio_replicated' AND name = 'p2' AND active);

SELECT 'materialize, plain vs no ratio',
    (SELECT arraySort(groupArray(tuple(hash_of_uncompressed_files, uncompressed_hash_of_compressed_files))) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_proj_ratio_plain' AND name = 'p2' AND active)
    = (SELECT arraySort(groupArray(tuple(hash_of_uncompressed_files, uncompressed_hash_of_compressed_files))) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_proj_no_ratio_replicated' AND name = 'p2' AND active);

SELECT count(), sum(a), sum(b), sum(length(s)) FROM t_proj_ratio_replicated;

DROP TABLE t_proj_ratio_replicated;
DROP TABLE t_proj_no_ratio_replicated;
DROP TABLE t_proj_ratio_plain;
