-- Patch parts of lightweight updates are applied on read to the rows they update, in several data parts and with several patches per part.

DROP TABLE IF EXISTS t_patch_parts_many;

CREATE TABLE t_patch_parts_many (id UInt64, v UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = 128;

SYSTEM STOP MERGES t_patch_parts_many;

INSERT INTO t_patch_parts_many SELECT number, number, '' FROM numbers(0, 1000);
INSERT INTO t_patch_parts_many SELECT number, number, '' FROM numbers(1000, 1000);
INSERT INTO t_patch_parts_many SELECT number, number, '' FROM numbers(2000, 1000);

SET apply_patch_parts = 1;

UPDATE t_patch_parts_many SET v = v + 1000000 WHERE id % 3 = 0;
UPDATE t_patch_parts_many SET s = 'b' WHERE id % 5 = 0;
UPDATE t_patch_parts_many SET v = 0, s = 'c' WHERE id % 7 = 0;
UPDATE t_patch_parts_many SET v = v + 1 WHERE id IN (0, 1, 999, 1000, 1999, 2000, 2999);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_patch_parts_many' AND startsWith(name, 'patch') AND active;

SELECT count(), sum(v), countIf(s = 'b'), countIf(s = 'c') FROM t_patch_parts_many;
SELECT id, v, s FROM t_patch_parts_many WHERE id IN (0, 1, 3, 5, 7, 999, 1000, 1999, 2000, 2999) ORDER BY id;
SELECT count(), sum(v) FROM t_patch_parts_many WHERE id % 3 = 0 AND id % 7 != 0;

SYSTEM START MERGES t_patch_parts_many;
OPTIMIZE TABLE t_patch_parts_many FINAL;

SELECT count(), sum(v), countIf(s = 'b'), countIf(s = 'c') FROM t_patch_parts_many;
SELECT id, v, s FROM t_patch_parts_many WHERE id IN (0, 1, 3, 5, 7, 999, 1000, 1999, 2000, 2999) ORDER BY id;

DROP TABLE t_patch_parts_many;
