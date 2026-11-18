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
SET optimize_use_projection_filtering_with_patch_parts = 1;

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
SELECT '-- the same with optimize_use_projection_filtering_with_patch_parts disabled: no projection is used';
SELECT count() FROM (EXPLAIN projections = 1 SELECT * FROM t_lw_proj_patch WHERE k = 5 SETTINGS optimize_use_projection_filtering_with_patch_parts = 0) WHERE explain LIKE '%Name: p_k%';
SELECT id, k, v FROM t_lw_proj_patch WHERE k = 5 AND v = 'b' ORDER BY id SETTINGS optimize_use_projection_filtering_with_patch_parts = 0;
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

-- A projection on a MATERIALIZED column depends on the columns of its expression:
-- updating `a` changes `m`, both in a patch part and in a mutation applied on the fly.
CREATE TABLE t_lw_proj_patch
(
    id UInt64,
    a UInt64,
    b UInt64,
    m UInt64 MATERIALIZED a * 2,
    PROJECTION p_m INDEX m TYPE basic
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    index_granularity = 16;

SYSTEM STOP MERGES t_lw_proj_patch;

INSERT INTO t_lw_proj_patch (id, a, b) SELECT number, number, number FROM numbers(1000);
INSERT INTO t_lw_proj_patch (id, a, b) SELECT number + 1000, number + 1000, number FROM numbers(1000);

SELECT '-- lightweight update of the source of a MATERIALIZED projection column';
UPDATE t_lw_proj_patch SET a = 1000000 WHERE id = 5;
SELECT id, a, m FROM t_lw_proj_patch WHERE m = 2000000;
SELECT id, a, m FROM t_lw_proj_patch WHERE m = 10;

SELECT '-- lightweight update of an unrelated column';
UPDATE t_lw_proj_patch SET b = 7 WHERE id = 1500;
SELECT id, a, b, m FROM t_lw_proj_patch WHERE m = 3000;

SELECT '-- mutation on the fly of the source of a MATERIALIZED projection column';
ALTER TABLE t_lw_proj_patch UPDATE a = 3000000 WHERE id = 1600 SETTINGS mutations_sync = 0, apply_mutations_on_fly = 1;
SELECT id, a, m FROM t_lw_proj_patch WHERE m = 6000000 SETTINGS apply_mutations_on_fly = 1;
SELECT id, a, m FROM t_lw_proj_patch WHERE m = 6000000 SETTINGS apply_mutations_on_fly = 1, optimize_use_projections = 0;
SELECT id, a, m FROM t_lw_proj_patch WHERE m = 3200 SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_lw_proj_patch;

-- A lightweight delete materialized in the part rebuilds the projection without the deleted rows.
-- A patch that restores such a row (`_row_exists = 1`) must not be filtered by that projection.
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

INSERT INTO t_lw_proj_patch SELECT number, number FROM numbers(1000);

SELECT '-- a patch restores a row that the rebuilt projection omits';
DELETE FROM t_lw_proj_patch WHERE id = 5 SETTINGS lightweight_delete_mode = 'alter_update', mutations_sync = 2;
SYSTEM STOP MERGES t_lw_proj_patch;
UPDATE t_lw_proj_patch SET _row_exists = 1 WHERE id = 5 SETTINGS apply_deleted_mask = 0, optimize_use_projections = 0;
SELECT id FROM t_lw_proj_patch WHERE k = 5;
SELECT id FROM t_lw_proj_patch WHERE k = 5 SETTINGS optimize_use_projections = 0;

DROP TABLE t_lw_proj_patch;

-- The query condition cache must not be consulted for the projection parts while the table has data changes on the fly:
-- a TopK entry recorded before a delete excludes the granules of the rows that become the new top rows.
SET optimize_read_in_order = 0;
SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;

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
INSERT INTO t_lw_proj_patch SELECT number, 9999 - number FROM numbers(10000) SETTINGS max_block_size = 10000;

SELECT '-- warm query condition cache, then a patch delete of the top rows';
SELECT k FROM t_lw_proj_patch WHERE k < 5000 ORDER BY k LIMIT 2 SETTINGS max_threads = 1, max_block_size = 16;
SELECT k FROM t_lw_proj_patch WHERE k < 5000 ORDER BY k LIMIT 2 SETTINGS max_threads = 1, max_block_size = 16;
DELETE FROM t_lw_proj_patch WHERE k < 1000 SETTINGS optimize_use_projections = 0;
SELECT k FROM t_lw_proj_patch WHERE k < 5000 ORDER BY k LIMIT 2 SETTINGS max_threads = 1, max_block_size = 16;

DROP TABLE t_lw_proj_patch;

CREATE TABLE t_lw_proj_patch
(
    id UInt64,
    k UInt64,
    PROJECTION p_k INDEX k TYPE basic
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 16;

SYSTEM STOP MERGES t_lw_proj_patch;
INSERT INTO t_lw_proj_patch SELECT number, 9999 - number FROM numbers(10000) SETTINGS max_block_size = 10000;

SELECT '-- warm query condition cache, then a mutation on the fly deleting the top rows';
SELECT k FROM t_lw_proj_patch WHERE k < 5000 ORDER BY k LIMIT 2 SETTINGS max_threads = 1, max_block_size = 16, apply_mutations_on_fly = 1;
SELECT k FROM t_lw_proj_patch WHERE k < 5000 ORDER BY k LIMIT 2 SETTINGS max_threads = 1, max_block_size = 16, apply_mutations_on_fly = 1;
ALTER TABLE t_lw_proj_patch DELETE WHERE k < 1000 SETTINGS mutations_sync = 0;
SELECT k FROM t_lw_proj_patch WHERE k < 5000 ORDER BY k LIMIT 2 SETTINGS max_threads = 1, max_block_size = 16, apply_mutations_on_fly = 1;

DROP TABLE t_lw_proj_patch;
