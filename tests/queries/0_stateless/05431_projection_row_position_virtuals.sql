-- A normal projection that does not store `_part_offset`, `_part_granule_offset`, `_block_number` or `_block_offset`
-- must not serve them: its rows are in another order (#120719, #121599).

SET optimize_use_projections = 1;

DROP TABLE IF EXISTS t_proj_offset;
CREATE TABLE t_proj_offset
(
    key UInt64,
    val UInt64,
    PROJECTION p_val (SELECT key, val ORDER BY val) WITH SETTINGS (index_granularity = 1)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;
INSERT INTO t_proj_offset SELECT number, 1000 - number FROM numbers(64);

SELECT _part_offset, _part_granule_offset FROM t_proj_offset WHERE key = 3 SETTINGS prefer_optimize_projection = 1;
SELECT sum(key) FROM t_proj_offset WHERE _part_offset = 3 SETTINGS prefer_optimize_projection = 1;
SELECT _part_offset FROM t_proj_offset WHERE key = 3 SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
SELECT _part_granule_offset FROM t_proj_offset WHERE key = 3 SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
-- the projection's part filtering must not prune by its own offsets either (a key set, so the exclusion search is used)
SELECT key FROM t_proj_offset WHERE val IN (997, 950) AND _part_offset = 3 SETTINGS optimize_use_projection_filtering = 1;

-- after a merge the projection wins on marks without any setting
DROP TABLE IF EXISTS t_proj_offset_merged;
CREATE TABLE t_proj_offset_merged
(
    x UInt64,
    payload UInt64,
    PROJECTION by_payload (SELECT x, payload ORDER BY payload)
)
ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 1, merge_selector_algorithm = 'Manual';
INSERT INTO t_proj_offset_merged SELECT 2 * number, number FROM numbers(8);
INSERT INTO t_proj_offset_merged SELECT 2 * number + 1, number + 8 FROM numbers(8);
OPTIMIZE TABLE t_proj_offset_merged FINAL;
SELECT x, payload, _part_offset FROM t_proj_offset_merged WHERE _part_offset = 1 ORDER BY payload;

-- a projection that stores the parent offset still serves it
DROP TABLE IF EXISTS t_proj_offset_stored;
CREATE TABLE t_proj_offset_stored
(
    key UInt64,
    val UInt64,
    PROJECTION p_val (SELECT key, val, _part_offset ORDER BY val) WITH SETTINGS (index_granularity = 1)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;
INSERT INTO t_proj_offset_stored SELECT number, 1000 - number FROM numbers(64);
SELECT _part_offset FROM t_proj_offset_stored WHERE val = 997 SETTINGS force_optimize_projection = 1;

-- the row selection of a lightweight update reads `_block_number`, `_block_offset` (and `_part_offset` for v1 patches)
DROP TABLE IF EXISTS t_proj_lwu_v1;
DROP TABLE IF EXISTS t_proj_lwu_v2;
DROP TABLE IF EXISTS t_proj_lwu_rmt;
CREATE TABLE t_proj_lwu_v1 (k UInt64, v Int64, s String, PROJECTION p (SELECT * ORDER BY v))
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = 3,
         patch_parts_version = 'v1', lightweight_mutation_projection_mode = 'rebuild';
CREATE TABLE t_proj_lwu_v2 (k UInt64, v Int64, s String, PROJECTION p (SELECT * ORDER BY v))
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = 3,
         patch_parts_version = 'v2', lightweight_mutation_projection_mode = 'rebuild';
CREATE TABLE t_proj_lwu_rmt (k UInt64, v Int64, s String, PROJECTION p (SELECT * ORDER BY v))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_proj_lwu_rmt', '1') ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = 3,
         patch_parts_version = 'v2', lightweight_mutation_projection_mode = 'rebuild';

INSERT INTO t_proj_lwu_v1 SELECT number, if(number % 2, 1, -1), 'old' FROM numbers(10);
INSERT INTO t_proj_lwu_v2 SELECT number, if(number % 2, 1, -1), 'old' FROM numbers(10);
INSERT INTO t_proj_lwu_rmt SELECT number, if(number % 2, 1, -1), 'old' FROM numbers(10);

-- each of these reads only one virtual the projection does not store
SELECT _block_number FROM t_proj_lwu_v2 WHERE v > 0 SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
SELECT _block_offset FROM t_proj_lwu_v2 WHERE v > 0 SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

UPDATE t_proj_lwu_v1 SET s = 'new' WHERE v > 0;
UPDATE t_proj_lwu_v2 SET s = 'new' WHERE v > 0;
UPDATE t_proj_lwu_rmt SET s = 'new' WHERE v > 0;
SELECT arraySort(groupArray(k)) FROM t_proj_lwu_v1 WHERE s = 'new';
SELECT arraySort(groupArray(k)) FROM t_proj_lwu_v2 WHERE s = 'new';
SELECT arraySort(groupArray(k)) FROM t_proj_lwu_rmt WHERE s = 'new';

DROP TABLE IF EXISTS t_proj_lwu_alter;
DROP TABLE IF EXISTS t_proj_lwu_delete;
CREATE TABLE t_proj_lwu_alter (k UInt64, v Int64, s String, PROJECTION p (SELECT * ORDER BY v))
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = 3,
         patch_parts_version = 'v2', lightweight_mutation_projection_mode = 'rebuild';
CREATE TABLE t_proj_lwu_delete (k UInt64, v Int64, s String, PROJECTION p (SELECT * ORDER BY v))
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = 3,
         patch_parts_version = 'v2', lightweight_mutation_projection_mode = 'rebuild';

INSERT INTO t_proj_lwu_alter SELECT number, if(number % 2, 1, -1), 'old' FROM numbers(10);
ALTER TABLE t_proj_lwu_alter UPDATE s = 'new' WHERE v > 0 SETTINGS alter_update_mode = 'lightweight';
SELECT arraySort(groupArray(k)) FROM t_proj_lwu_alter WHERE s = 'new';

INSERT INTO t_proj_lwu_delete SELECT number, if(number % 2, 1, -1), 'old' FROM numbers(10);
DELETE FROM t_proj_lwu_delete WHERE v > 0 SETTINGS lightweight_delete_mode = 'lightweight_update';
SELECT arraySort(groupArray(k)) FROM t_proj_lwu_delete;

DROP TABLE t_proj_offset;
DROP TABLE t_proj_offset_merged;
DROP TABLE t_proj_offset_stored;
DROP TABLE t_proj_lwu_v1;
DROP TABLE t_proj_lwu_v2;
DROP TABLE t_proj_lwu_rmt;
DROP TABLE t_proj_lwu_alter;
DROP TABLE t_proj_lwu_delete;
