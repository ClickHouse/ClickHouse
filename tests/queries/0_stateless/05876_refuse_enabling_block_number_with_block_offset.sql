-- Enabling `enable_block_number_column` on a table that already has `enable_block_offset_column` enabled is refused:
-- parts merged in between store `_block_offset` without `_block_number`, which gives duplicates in
-- (`_block_number`, `_block_offset`) and makes lightweight updates modify wrong rows.

DROP TABLE IF EXISTS t_block_number_after_offset;

CREATE TABLE t_block_number_after_offset (id UInt64, v String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1;

INSERT INTO t_block_number_after_offset SELECT number, 'a' FROM numbers(5);
INSERT INTO t_block_number_after_offset SELECT number + 5, 'b' FROM numbers(5);
OPTIMIZE TABLE t_block_number_after_offset FINAL;

-- This is the state that must not get `enable_block_number_column`: the pairs are not unique.
SELECT count(), uniqExact(_block_number, _block_offset) FROM t_block_number_after_offset;

ALTER TABLE t_block_number_after_offset MODIFY SETTING enable_block_number_column = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_block_number_after_offset MODIFY SETTING allow_experimental_block_number_column = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_block_number_after_offset MODIFY SETTING enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_block_number_after_offset MODIFY SETTING enable_block_number_column = 1, enable_block_offset_column = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_block_number_after_offset ADD COLUMN c UInt8, MODIFY SETTING enable_block_number_column = 1; -- { serverError BAD_ARGUMENTS }

-- Other alters of such a table are not affected.
ALTER TABLE t_block_number_after_offset MODIFY SETTING enable_block_number_column = 0;
ALTER TABLE t_block_number_after_offset ADD COLUMN c UInt8;

SELECT extractAll(engine_full, 'enable_block_\\w+_column = \\w+') FROM system.tables WHERE database = currentDatabase() AND name = 't_block_number_after_offset';

DROP TABLE t_block_number_after_offset;

-- A table without data has no parts with duplicates, so there it is allowed.
CREATE TABLE t_block_number_after_offset (id UInt64, v String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1;

ALTER TABLE t_block_number_after_offset MODIFY SETTING enable_block_number_column = 1;
ALTER TABLE t_block_number_after_offset MODIFY SETTING enable_block_number_column = 0;

INSERT INTO t_block_number_after_offset SELECT number, 'a' FROM numbers(5);
ALTER TABLE t_block_number_after_offset MODIFY SETTING enable_block_number_column = 1; -- { serverError BAD_ARGUMENTS }

TRUNCATE TABLE t_block_number_after_offset;
ALTER TABLE t_block_number_after_offset MODIFY SETTING enable_block_number_column = 1;

DROP TABLE t_block_number_after_offset;

-- Enabling both settings at once, or `enable_block_offset_column` after `enable_block_number_column`, is still allowed.
CREATE TABLE t_block_number_after_offset (id UInt64, v String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_block_number_after_offset SELECT number, 'a' FROM numbers(5);
INSERT INTO t_block_number_after_offset SELECT number + 5, 'b' FROM numbers(5);
OPTIMIZE TABLE t_block_number_after_offset FINAL;

ALTER TABLE t_block_number_after_offset MODIFY SETTING enable_block_number_column = 1, enable_block_offset_column = 1;
SELECT count(), uniqExact(_block_number, _block_offset) FROM t_block_number_after_offset;

DROP TABLE t_block_number_after_offset;

CREATE TABLE t_block_number_after_offset (id UInt64, v String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0;

ALTER TABLE t_block_number_after_offset MODIFY SETTING enable_block_number_column = 1;
ALTER TABLE t_block_number_after_offset MODIFY SETTING enable_block_offset_column = 1;

DROP TABLE t_block_number_after_offset;
