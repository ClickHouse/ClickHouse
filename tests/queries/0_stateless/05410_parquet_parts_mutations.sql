-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- Mutations, lightweight deletes, and ALTERs of data parts in Parquet format, and changes of the format of parts.

SET mutations_sync = 2, alter_sync = 2;

DROP TABLE IF EXISTS t_parquet_mutations;

CREATE TABLE t_parquet_mutations (key UInt64, a String, b UInt32)
ENGINE = MergeTree ORDER BY key
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 100;

INSERT INTO t_parquet_mutations SELECT number, toString(number), number FROM numbers(1000);

SELECT 'update';
ALTER TABLE t_parquet_mutations UPDATE b = b * 2 WHERE key % 2 = 0;
SELECT sum(b), count() FROM t_parquet_mutations;

SELECT 'delete';
ALTER TABLE t_parquet_mutations DELETE WHERE key % 3 = 0;
SELECT sum(b), count() FROM t_parquet_mutations;

SELECT 'lightweight delete';
DELETE FROM t_parquet_mutations WHERE key % 5 = 0;
SELECT sum(b), count() FROM t_parquet_mutations;

SELECT 'add column';
ALTER TABLE t_parquet_mutations ADD COLUMN c String DEFAULT 'x' || toString(key);
SELECT c FROM t_parquet_mutations WHERE key = 7;
ALTER TABLE t_parquet_mutations MATERIALIZE COLUMN c;
SELECT c FROM t_parquet_mutations WHERE key = 7;

SELECT 'rename column';
ALTER TABLE t_parquet_mutations RENAME COLUMN a TO a2;
SELECT a2 FROM t_parquet_mutations WHERE key = 7;

SELECT 'modify column';
ALTER TABLE t_parquet_mutations MODIFY COLUMN b UInt64;
SELECT toTypeName(b), sum(b) FROM t_parquet_mutations GROUP BY 1;

SELECT 'drop column';
ALTER TABLE t_parquet_mutations DROP COLUMN c;
SELECT * FROM t_parquet_mutations WHERE key = 7;

SELECT 'skip index';
ALTER TABLE t_parquet_mutations ADD INDEX idx a2 TYPE bloom_filter GRANULARITY 1;
ALTER TABLE t_parquet_mutations MATERIALIZE INDEX idx;
SELECT key FROM t_parquet_mutations WHERE a2 = '701' SETTINGS force_data_skipping_indices = 'idx';

SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_mutations' AND active;
CHECK TABLE t_parquet_mutations SETTINGS check_query_single_value_result = 1;

SELECT 'to Wide';
ALTER TABLE t_parquet_mutations MODIFY SETTING min_rows_for_parquet_part = 0, min_bytes_for_wide_part = 0;
OPTIMIZE TABLE t_parquet_mutations FINAL;
SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_mutations' AND active;
SELECT sum(b), count(), sum(length(a2)) FROM t_parquet_mutations;

SELECT 'to Parquet';
ALTER TABLE t_parquet_mutations MODIFY SETTING min_rows_for_parquet_part = 1;
INSERT INTO t_parquet_mutations VALUES (1000000, 'x', 0);
OPTIMIZE TABLE t_parquet_mutations FINAL;
SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_mutations' AND active;
SELECT sum(b), count(), sum(length(a2)) FROM t_parquet_mutations;

SELECT 'detach and attach';
DETACH TABLE t_parquet_mutations;
ATTACH TABLE t_parquet_mutations;
SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_mutations' AND active;
SELECT sum(b), count(), sum(length(a2)) FROM t_parquet_mutations;
CHECK TABLE t_parquet_mutations SETTINGS check_query_single_value_result = 1;

DROP TABLE t_parquet_mutations;
