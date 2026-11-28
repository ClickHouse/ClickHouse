-- Disabling `enable_block_number_column` or `enable_block_offset_column` on a table with data is refused.
-- The existing parts keep the columns they were written with, so disabling `enable_block_offset_column`,
-- enabling `enable_block_number_column` and enabling `enable_block_offset_column` back would give a table with
-- both settings and with duplicates in (`_block_number`, `_block_offset`), where lightweight updates modify wrong rows.

DROP TABLE IF EXISTS t_disable_block_columns;

CREATE TABLE t_disable_block_columns (id UInt64, v String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1;

INSERT INTO t_disable_block_columns SELECT number, 'a' FROM numbers(5);
INSERT INTO t_disable_block_columns SELECT number + 5, 'b' FROM numbers(5);
OPTIMIZE TABLE t_disable_block_columns FINAL;

SELECT count(), uniqExact(_block_number, _block_offset) FROM t_disable_block_columns;

-- The first step of the sequence is refused, in every spelling.
ALTER TABLE t_disable_block_columns MODIFY SETTING enable_block_offset_column = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_disable_block_columns MODIFY SETTING enable_block_offset_column = DEFAULT; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_disable_block_columns RESET SETTING enable_block_offset_column; -- { serverError BAD_ARGUMENTS }

SELECT extractAll(engine_full, 'enable_block_\\w+_column = \\w+') FROM system.tables WHERE database = currentDatabase() AND name = 't_disable_block_columns';

DROP TABLE t_disable_block_columns;

-- The same for a table with both settings, for each of them and under each name.
CREATE TABLE t_disable_block_columns (id UInt64, v String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_disable_block_columns SELECT number, 'a' FROM numbers(5);

ALTER TABLE t_disable_block_columns MODIFY SETTING enable_block_number_column = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_disable_block_columns MODIFY SETTING allow_experimental_block_number_column = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_disable_block_columns RESET SETTING enable_block_number_column; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_disable_block_columns RESET SETTING allow_experimental_block_number_column; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_disable_block_columns MODIFY SETTING enable_block_offset_column = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_disable_block_columns RESET SETTING enable_block_offset_column; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_disable_block_columns MODIFY SETTING enable_block_number_column = 0, enable_block_offset_column = 0; -- { serverError BAD_ARGUMENTS }

-- Alters that keep the settings are not affected.
ALTER TABLE t_disable_block_columns MODIFY SETTING enable_block_number_column = 1, enable_block_offset_column = 1;
ALTER TABLE t_disable_block_columns ADD COLUMN c UInt8;

SELECT extractAll(engine_full, 'enable_block_\\w+_column = \\w+') FROM system.tables WHERE database = currentDatabase() AND name = 't_disable_block_columns';

-- A table without data has no parts, so there the settings can be disabled.
TRUNCATE TABLE t_disable_block_columns;
ALTER TABLE t_disable_block_columns MODIFY SETTING enable_block_number_column = 0;
ALTER TABLE t_disable_block_columns RESET SETTING enable_block_offset_column;

SELECT extractAll(engine_full, 'enable_block_\\w+_column = \\w+') FROM system.tables WHERE database = currentDatabase() AND name = 't_disable_block_columns';

DROP TABLE t_disable_block_columns;
