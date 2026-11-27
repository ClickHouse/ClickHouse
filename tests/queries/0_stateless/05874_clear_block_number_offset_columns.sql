-- Tags: zookeeper

-- `ALTER TABLE ... CLEAR COLUMN _block_number | _block_offset` resets both columns: a part then reads
-- `_block_number` as its `min_block` and `_block_offset` as `_part_offset`.

SET mutations_sync = 2;
SET alter_sync = 2;

-- A merge with `enable_block_offset_column = 1` and `enable_block_number_column = 0` stores `_block_offset`
-- without `_block_number`, so the pairs `(_block_number, _block_offset)` repeat. A Wide part is mutated through
-- hardlinks, a Compact and a Packed one are rewritten.

DROP TABLE IF EXISTS t_clear_block_wide;
DROP TABLE IF EXISTS t_clear_block_compact;
DROP TABLE IF EXISTS t_clear_block_packed;

CREATE TABLE t_clear_block_wide (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_clear_block_compact (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_clear_block_packed (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = '10G';

INSERT INTO t_clear_block_wide VALUES (1, 'a'), (2, 'b');
INSERT INTO t_clear_block_wide VALUES (3, 'c'), (4, 'd');
INSERT INTO t_clear_block_compact VALUES (1, 'a'), (2, 'b');
INSERT INTO t_clear_block_compact VALUES (3, 'c'), (4, 'd');
INSERT INTO t_clear_block_packed VALUES (1, 'a'), (2, 'b');
INSERT INTO t_clear_block_packed VALUES (3, 'c'), (4, 'd');

OPTIMIZE TABLE t_clear_block_wide FINAL;
OPTIMIZE TABLE t_clear_block_compact FINAL;
OPTIMIZE TABLE t_clear_block_packed FINAL;

SELECT 'before';
SELECT table, part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table LIKE 't_clear_block_%' AND active ORDER BY table;
SELECT 'wide', id, _block_number, _block_offset FROM t_clear_block_wide ORDER BY id;
SELECT 'compact', id, _block_number, _block_offset FROM t_clear_block_compact ORDER BY id;
SELECT 'packed', id, _block_number, _block_offset FROM t_clear_block_packed ORDER BY id;

ALTER TABLE t_clear_block_wide CLEAR COLUMN _block_number;
ALTER TABLE t_clear_block_compact CLEAR COLUMN _block_number;
ALTER TABLE t_clear_block_packed CLEAR COLUMN _block_number;

SELECT 'after';
SELECT table, part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table LIKE 't_clear_block_%' AND active ORDER BY table;
-- The columns are removed from the parts.
SELECT table, column FROM system.parts_columns WHERE database = currentDatabase() AND table LIKE 't_clear_block_%' AND active AND column LIKE '\\_block\\_%' ORDER BY table, column;
SELECT 'wide', id, _block_number, _block_offset FROM t_clear_block_wide ORDER BY id;
SELECT 'compact', id, _block_number, _block_offset FROM t_clear_block_compact ORDER BY id;
SELECT 'packed', id, _block_number, _block_offset FROM t_clear_block_packed ORDER BY id;

-- The parts are reloaded from disk.
DETACH TABLE t_clear_block_wide;
ATTACH TABLE t_clear_block_wide;
DETACH TABLE t_clear_block_compact;
ATTACH TABLE t_clear_block_compact;
DETACH TABLE t_clear_block_packed;
ATTACH TABLE t_clear_block_packed;

SELECT 'after reload';
SELECT 'wide', id, _block_number, _block_offset FROM t_clear_block_wide ORDER BY id;
SELECT 'compact', id, _block_number, _block_offset FROM t_clear_block_compact ORDER BY id;
SELECT 'packed', id, _block_number, _block_offset FROM t_clear_block_packed ORDER BY id;

-- The next merge stores the values the parts read.
ALTER TABLE t_clear_block_wide MODIFY SETTING enable_block_number_column = 1;
INSERT INTO t_clear_block_wide VALUES (5, 'e');
OPTIMIZE TABLE t_clear_block_wide FINAL;

SELECT 'after merge';
SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_clear_block_wide' AND active AND column LIKE '\\_block\\_%' ORDER BY column;
SELECT id, _block_number, _block_offset FROM t_clear_block_wide ORDER BY id;

DROP TABLE t_clear_block_wide;
DROP TABLE t_clear_block_compact;
DROP TABLE t_clear_block_packed;

-- Either name clears both columns. The part min/max index gets the ranges of the values the part reads now. The skip
-- indices and the projections built from the stored values are removed from the part, and the next merge builds them again.

DROP TABLE IF EXISTS t_clear_block_deps;

CREATE TABLE t_clear_block_deps (id UInt64, s String, PROJECTION p_by_block (SELECT _block_number, _block_offset, id ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, part_minmax_index_columns = 'with_block_number_offset',
    add_minmax_index_for_block_number_column = 1, add_minmax_index_for_block_offset_column = 1, allow_commit_order_projection = 1, index_granularity = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_clear_block_deps VALUES (1, 'a'), (2, 'b');
INSERT INTO t_clear_block_deps VALUES (3, 'c'), (4, 'd');
OPTIMIZE TABLE t_clear_block_deps FINAL;

SELECT 'deps before';
SELECT id, _block_number, _block_offset FROM t_clear_block_deps ORDER BY id;
SELECT secondary_indices_compressed_bytes > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_clear_block_deps' AND active;
SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_clear_block_deps' AND active;

ALTER TABLE t_clear_block_deps CLEAR COLUMN _block_offset;

SELECT 'deps after';
SELECT id, _block_number, _block_offset FROM t_clear_block_deps ORDER BY id;
SELECT secondary_indices_compressed_bytes > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_clear_block_deps' AND active;
SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_clear_block_deps' AND active;
SELECT id FROM t_clear_block_deps WHERE _block_offset = 3 ORDER BY id;
SELECT id FROM t_clear_block_deps WHERE _block_number = 1 ORDER BY id;
SELECT count() FROM t_clear_block_deps WHERE _block_number = 2;
SELECT id FROM t_clear_block_deps WHERE _block_offset = 3 ORDER BY id SETTINGS use_skip_indexes = 0, optimize_use_projections = 0;

DETACH TABLE t_clear_block_deps;
ATTACH TABLE t_clear_block_deps;

SELECT 'deps after reload';
SELECT id FROM t_clear_block_deps WHERE _block_offset = 3 ORDER BY id;
SELECT id FROM t_clear_block_deps WHERE _block_number = 1 ORDER BY id;

OPTIMIZE TABLE t_clear_block_deps FINAL;

SELECT 'deps after merge';
SELECT secondary_indices_compressed_bytes > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_clear_block_deps' AND active;
SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_clear_block_deps' AND active;
SELECT id, _block_number, _block_offset FROM t_clear_block_deps WHERE _block_offset = 3 ORDER BY id;

DROP TABLE t_clear_block_deps;

-- `IN PARTITION` clears the parts of that partition only.

DROP TABLE IF EXISTS t_clear_block_partition;

CREATE TABLE t_clear_block_partition (p UInt8, id UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_clear_block_partition VALUES (0, 1), (0, 2);
INSERT INTO t_clear_block_partition VALUES (1, 1), (1, 2);
INSERT INTO t_clear_block_partition VALUES (0, 3), (0, 4);
INSERT INTO t_clear_block_partition VALUES (1, 3), (1, 4);
OPTIMIZE TABLE t_clear_block_partition PARTITION 0 FINAL;
OPTIMIZE TABLE t_clear_block_partition PARTITION 1 FINAL;

ALTER TABLE t_clear_block_partition CLEAR COLUMN _block_number IN PARTITION 0;

SELECT 'partition';
SELECT p, id, _block_number, _block_offset FROM t_clear_block_partition ORDER BY p, id;

DROP TABLE t_clear_block_partition;

-- Together with another command of the same mutation, which then does not store `_block_number`, though
-- `enable_block_number_column` is enabled. And with `IF EXISTS`.

DROP TABLE IF EXISTS t_clear_block_combined;

CREATE TABLE t_clear_block_combined (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_clear_block_combined VALUES (1, 'a'), (2, 'b');
INSERT INTO t_clear_block_combined VALUES (3, 'c'), (4, 'd');
OPTIMIZE TABLE t_clear_block_combined FINAL;

ALTER TABLE t_clear_block_combined MODIFY SETTING enable_block_number_column = 1;
ALTER TABLE t_clear_block_combined CLEAR COLUMN s, CLEAR COLUMN IF EXISTS _block_number;

SELECT 'combined';
SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_clear_block_combined' AND active AND column LIKE '\\_block\\_%' ORDER BY column;
SELECT id, s, _block_number, _block_offset FROM t_clear_block_combined ORDER BY id;

DROP TABLE t_clear_block_combined;

-- All replicas of a replicated table get the same values.

DROP TABLE IF EXISTS t_clear_block_r1;
DROP TABLE IF EXISTS t_clear_block_r2;

CREATE TABLE t_clear_block_r1 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_block', 'r1') ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
CREATE TABLE t_clear_block_r2 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_block', 'r2') ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_clear_block_r1 VALUES (1), (2);
INSERT INTO t_clear_block_r1 VALUES (3), (4);
OPTIMIZE TABLE t_clear_block_r1 FINAL;
SYSTEM SYNC REPLICA t_clear_block_r2;

ALTER TABLE t_clear_block_r1 CLEAR COLUMN _block_number;
SYSTEM SYNC REPLICA t_clear_block_r2;

SELECT 'replicated';
-- The first block number of a replicated table depends on the environment, so it is compared with the one of the part.
SELECT 'r1', id, _block_number = (SELECT min_block_number FROM system.parts WHERE database = currentDatabase() AND table = 't_clear_block_r1' AND active), _block_offset
FROM t_clear_block_r1 ORDER BY id;
SELECT 'r2', id, _block_number = (SELECT min_block_number FROM system.parts WHERE database = currentDatabase() AND table = 't_clear_block_r2' AND active), _block_offset
FROM t_clear_block_r2 ORDER BY id;

DROP TABLE t_clear_block_r1;
DROP TABLE t_clear_block_r2;

-- The columns stay in the table.

DROP TABLE IF EXISTS t_clear_block_rejected;

CREATE TABLE t_clear_block_rejected (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

ALTER TABLE t_clear_block_rejected DROP COLUMN _block_number; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
ALTER TABLE t_clear_block_rejected DROP COLUMN _block_offset; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
ALTER TABLE t_clear_block_rejected CLEAR COLUMN _part_offset; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

DROP TABLE t_clear_block_rejected;
