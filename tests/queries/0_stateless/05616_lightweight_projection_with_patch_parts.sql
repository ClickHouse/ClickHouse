-- Tags: no-parallel-replicas
-- no-parallel-replicas: the plan with projections differs with parallel replicas.

-- A lightweight projection (`_part_offset`) filters the parent read even when the table has patch parts,
-- unless a patch of the part changes a column the projection depends on.

SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update';
SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;
SET min_table_rows_to_use_projection_index = 0;
SET max_projection_rows_to_use_projection_index = 1000000;

DROP TABLE IF EXISTS t_lw_proj_patch;

CREATE TABLE t_lw_proj_patch
(
    id UInt64,
    k UInt64,
    v String,
    PROJECTION p_k INDEX k TYPE basic
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 0,
    lightweight_mutation_projection_mode = 'rebuild',
    index_granularity = 16;

SYSTEM STOP MERGES t_lw_proj_patch;

INSERT INTO t_lw_proj_patch SELECT number, number % 100, 'a' FROM numbers(1000);
INSERT INTO t_lw_proj_patch SELECT number + 1000, number % 100, 'a' FROM numbers(1000);

SELECT '-- no patches';
SELECT trimLeft(explain) FROM (EXPLAIN projections = 1 SELECT * FROM t_lw_proj_patch WHERE k = 5) WHERE explain LIKE '%Name: p_k%' OR explain LIKE '%Description:%';
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 5 ORDER BY id LIMIT 3;

SELECT '-- update of a column the projection does not use';
UPDATE t_lw_proj_patch SET v = 'b' WHERE id IN (5, 1005);
SELECT trimLeft(explain) FROM (EXPLAIN projections = 1 SELECT * FROM t_lw_proj_patch WHERE k = 5) WHERE explain LIKE '%Name: p_k%' OR explain LIKE '%Description:%';
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 5 ORDER BY id LIMIT 3;
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 5 AND v = 'b' ORDER BY id;
SELECT count(), sum(id) FROM t_lw_proj_patch WHERE k = 5 AND v = 'a';
SELECT count(), sum(id) FROM t_lw_proj_patch WHERE k = 5 AND v = 'a' SETTINGS optimize_use_projections = 0;

SELECT '-- the projection covers the query, but reading it would miss the patches';
SELECT id, k FROM t_lw_proj_patch WHERE k = 5 ORDER BY id LIMIT 3;

SELECT '-- lightweight delete';
DELETE FROM t_lw_proj_patch WHERE id = 7;
SELECT trimLeft(explain) FROM (EXPLAIN projections = 1 SELECT * FROM t_lw_proj_patch WHERE k = 7) WHERE explain LIKE '%Name: p_k%' OR explain LIKE '%Description:%';
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 7 ORDER BY id LIMIT 3;
SELECT count() FROM t_lw_proj_patch WHERE k = 7;

SELECT '-- update of the projection key: the first part is read without the projection';
UPDATE t_lw_proj_patch SET k = 777 WHERE id = 6;
SELECT trimLeft(explain) FROM (EXPLAIN projections = 1 SELECT * FROM t_lw_proj_patch WHERE k = 777) WHERE explain LIKE '%Name: p_k%' OR explain LIKE '%Description:%' OR explain LIKE '%Parts:%';
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 777 ORDER BY id;
SELECT id FROM t_lw_proj_patch WHERE k = 6 ORDER BY id LIMIT 3;
SELECT count(), sum(id) FROM t_lw_proj_patch WHERE k IN (5, 6, 7, 777);
SELECT count(), sum(id) FROM t_lw_proj_patch WHERE k IN (5, 6, 7, 777) SETTINGS optimize_use_projections = 0;

SELECT '-- patches applied by join after a merge rebuilt the projection from the old values';
SYSTEM START MERGES t_lw_proj_patch;
OPTIMIZE TABLE t_lw_proj_patch FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lw_proj_patch' AND active AND NOT startsWith(partition_id, 'patch');
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 777 ORDER BY id;
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 5 AND v = 'b' ORDER BY id;
SELECT count(), sum(id) FROM t_lw_proj_patch WHERE k IN (5, 6, 7, 777);

SELECT '-- patches materialized';
ALTER TABLE t_lw_proj_patch APPLY PATCHES SETTINGS mutations_sync = 2;
SELECT trimLeft(explain) FROM (EXPLAIN projections = 1 SELECT * FROM t_lw_proj_patch WHERE k = 777) WHERE explain LIKE '%Name: p_k%' OR explain LIKE '%Description:%';
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 777 ORDER BY id;
SELECT count(), sum(id) FROM t_lw_proj_patch WHERE k IN (5, 6, 7, 777);

DROP TABLE t_lw_proj_patch;

-- A projection with WHERE depends on the columns of its WHERE as well.
CREATE TABLE t_lw_proj_patch
(
    id UInt64,
    k UInt64,
    v String,
    PROJECTION p_k (SELECT _part_offset WHERE v = 'a' ORDER BY k)
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    index_granularity = 16;

SYSTEM STOP MERGES t_lw_proj_patch;

INSERT INTO t_lw_proj_patch SELECT number, number % 100, if(number % 2 = 0, 'a', 'z') FROM numbers(1000);

SELECT '-- projection with WHERE, update of a column of its WHERE';
UPDATE t_lw_proj_patch SET v = 'a' WHERE id = 1;
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 1 AND v = 'a' ORDER BY id;

DROP TABLE t_lw_proj_patch;

-- Data mutations applied on the fly are handled the same way as patch parts.
CREATE TABLE t_lw_proj_patch
(
    id UInt64,
    k UInt64,
    v String,
    PROJECTION p_k INDEX k TYPE basic
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 16;

SYSTEM STOP MERGES t_lw_proj_patch;

INSERT INTO t_lw_proj_patch SELECT number, number % 100, 'a' FROM numbers(1000);

SET apply_mutations_on_fly = 1;

SELECT '-- mutation on the fly of a column the projection does not use';
ALTER TABLE t_lw_proj_patch UPDATE v = 'm' WHERE id = 9 SETTINGS mutations_sync = 0;
SELECT trimLeft(explain) FROM (EXPLAIN projections = 1 SELECT * FROM t_lw_proj_patch WHERE k = 9) WHERE explain LIKE '%Name: p_k%' OR explain LIKE '%Description:%';
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 9 ORDER BY id LIMIT 3;

SELECT '-- mutation on the fly of the projection key';
ALTER TABLE t_lw_proj_patch UPDATE k = 888 WHERE id = 10 SETTINGS mutations_sync = 0;
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 888 ORDER BY id;
SELECT id FROM t_lw_proj_patch WHERE k = 10 ORDER BY id LIMIT 3;

DROP TABLE t_lw_proj_patch;

-- A delete leaves extra rows in the projection: ORDER BY ... LIMIT must still find the smallest live value.
CREATE TABLE t_lw_proj_patch
(
    id UInt64,
    k UInt64,
    PROJECTION p_k INDEX k TYPE basic
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    lightweight_mutation_projection_mode = 'rebuild',
    index_granularity = 16;

SYSTEM STOP MERGES t_lw_proj_patch;

INSERT INTO t_lw_proj_patch SELECT number, number FROM numbers(32);
INSERT INTO t_lw_proj_patch SELECT number + 1000, number + 100 FROM numbers(1000);

SELECT '-- ORDER BY LIMIT after deleting all the smallest values';
DELETE FROM t_lw_proj_patch WHERE id < 32;
SELECT id, k FROM t_lw_proj_patch WHERE k < 500 ORDER BY k LIMIT 2;
SELECT id, k FROM t_lw_proj_patch WHERE k < 500 ORDER BY k LIMIT 2 SETTINGS optimize_use_projections = 0;

DROP TABLE t_lw_proj_patch;
