-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks the shape of the local query plan.

-- Lazy FINAL reads the rows it selected again by row address. That read cannot apply patch parts and is misdirected
-- by a column named like the address, so lazy FINAL must fall back to a regular FINAL read then.

SET query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0, apply_patch_parts = 1;

DROP TABLE IF EXISTS t_lazy_final_patch;

CREATE TABLE t_lazy_final_patch (k UInt64, p UInt8, a UInt32)
ENGINE = ReplacingMergeTree
PARTITION BY p
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = 3, patch_parts_version = 'v2';

SYSTEM STOP MERGES t_lazy_final_patch;

INSERT INTO t_lazy_final_patch SELECT number, number % 4, number FROM numbers(100);
-- A part whose key range does not intersect the others.
INSERT INTO t_lazy_final_patch SELECT number, 0, number FROM numbers(1000, 10);

-- Patch parts only in partitions 1 and 2.
UPDATE t_lazy_final_patch SET a = a + 1000 WHERE k IN (5, 6);
SELECT 'update', count(), sum(a) FROM t_lazy_final_patch FINAL WHERE a IN (5, 6, 7, 1005, 1006);

DELETE FROM t_lazy_final_patch WHERE k = 7 SETTINGS lightweight_delete_mode = 'lightweight_update_force';
SELECT 'delete', count(), sum(a) FROM t_lazy_final_patch FINAL WHERE a IN (5, 6, 7, 1005, 1006);

SELECT 'plan with patches', countIf(explain LIKE '%InputSelector%') > 0 FROM (EXPLAIN SELECT count() FROM t_lazy_final_patch FINAL WHERE a IN (7, 8, 9));
SELECT 'plan without patches', countIf(explain LIKE '%InputSelector%') > 0 FROM (EXPLAIN SELECT count() FROM t_lazy_final_patch FINAL WHERE a IN (7, 8, 9) SETTINGS apply_patch_parts = 0);

DROP TABLE t_lazy_final_patch;

DROP TABLE IF EXISTS t_lazy_final_row_address;

-- A Nullable key skips the split into intersecting and non-intersecting parts.
CREATE TABLE t_lazy_final_row_address (k Nullable(UInt64), a UInt32, _part_offset UInt64 DEFAULT 0, _part_starting_offset UInt64 DEFAULT 0)
ENGINE = ReplacingMergeTree
ORDER BY k
SETTINGS allow_nullable_key = 1, index_granularity = 3;

SYSTEM STOP MERGES t_lazy_final_row_address;

INSERT INTO t_lazy_final_row_address (k, a) SELECT number, number FROM numbers(100);
INSERT INTO t_lazy_final_row_address (k, a) SELECT number, number + 1000 FROM numbers(50, 50);

SELECT 'row address', groupArray(a) FROM (SELECT a FROM t_lazy_final_row_address FINAL WHERE a IN (7, 60, 1060) ORDER BY a);

DROP TABLE t_lazy_final_row_address;
