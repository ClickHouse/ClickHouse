-- The rewritten (alternative) form of a key must not be matched by the name of another column of the table:
-- a subcolumn name that resolves through another column (`j.m.key_k` through the `JSON` column `j`, while
-- the key reads the column `j.m`), or a rewritten expression named as another column (`if(greater(v, 0), v, 0)`).
-- Related to issue #103128.

SET use_query_condition_cache = 0;

-- A subcolumn read that resolves through another column of the table.
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_json_pk;
DROP TABLE IF EXISTS t_json_idx;
DROP TABLE IF EXISTS t_json_part;

CREATE TABLE t_json_pk (j JSON(m Map(String, Int32)), `j.m` Map(String, Int32))
ENGINE = MergeTree ORDER BY `j.m`['k']
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_json_pk SELECT CAST(concat('{"m":{"k":', toString(number), '}}'), 'JSON(m Map(String, Int32))'), map('k', toInt32(100 + number)) FROM numbers(100);

SELECT count() FROM t_json_pk WHERE `j.m.key_k` = 99;
SELECT count() FROM t_json_pk WHERE `j.m.key_k` = 99 SETTINGS use_primary_key = 0;

CREATE TABLE t_json_idx (j JSON(m Map(String, Int32)), `j.m` Map(String, Int32), INDEX idx `j.m`['k'] TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_json_idx SELECT CAST(concat('{"m":{"k":', toString(number), '}}'), 'JSON(m Map(String, Int32))'), map('k', toInt32(100 + number)) FROM numbers(100);

SELECT count() FROM t_json_idx WHERE `j.m.key_k` = 99;
SELECT count() FROM t_json_idx WHERE `j.m.key_k` = 99 SETTINGS use_skip_indexes = 0;

CREATE TABLE t_json_part (j JSON(m Map(String, Int32)), `j.m` Map(String, Int32))
ENGINE = MergeTree PARTITION BY `j.m`['k'] ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_json_part SELECT CAST(concat('{"m":{"k":', toString(number), '}}'), 'JSON(m Map(String, Int32))'), map('k', toInt32(100 + number)) FROM numbers(10);

SELECT count() FROM t_json_part WHERE `j.m.key_k` = 9;
SELECT count() FROM t_json_part WHERE `j.m.key_k` = 9 SETTINGS use_partition_pruning = 0;

-- Partition pruning of a mutation.
ALTER TABLE t_json_part DELETE WHERE `j.m.key_k` = 9 SETTINGS mutations_sync = 2;
SELECT count() FROM t_json_part WHERE `j.m.key_k` = 9 SETTINGS use_partition_pruning = 0;

DROP TABLE t_json_pk;
DROP TABLE t_json_idx;
DROP TABLE t_json_part;

-- A rewritten expression named as another column of the table.
SET optimize_functions_to_subcolumns = 0;
SET optimize_multiif_to_if = 1;

DROP TABLE IF EXISTS t_name_pk;
DROP TABLE IF EXISTS t_name_idx;
DROP TABLE IF EXISTS t_name_part;

CREATE TABLE t_name_pk (v Int32, `if(greater(v, 0), v, 0)` Int32)
ENGINE = MergeTree ORDER BY multiIf(v > 0, v, 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_name_pk SELECT 100 + number, number FROM numbers(100);

SELECT count() FROM t_name_pk WHERE `if(greater(v, 0), v, 0)` = 99;
SELECT count() FROM t_name_pk WHERE `if(greater(v, 0), v, 0)` = 99 SETTINGS use_primary_key = 0;

CREATE TABLE t_name_idx (v Int32, `if(greater(v, 0), v, 0)` Int32, INDEX idx multiIf(v > 0, v, 0) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_name_idx SELECT 100 + number, number FROM numbers(100);

SELECT count() FROM t_name_idx WHERE `if(greater(v, 0), v, 0)` = 99;
SELECT count() FROM t_name_idx WHERE `if(greater(v, 0), v, 0)` = 99 SETTINGS use_skip_indexes = 0;

CREATE TABLE t_name_part (v Int32, `if(greater(v, 0), v, 0)` Int32)
ENGINE = MergeTree PARTITION BY multiIf(v > 0, v, 0) ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_name_part SELECT 100 + number, number FROM numbers(10);

SELECT count() FROM t_name_part WHERE `if(greater(v, 0), v, 0)` = 9;
SELECT count() FROM t_name_part WHERE `if(greater(v, 0), v, 0)` = 9 SETTINGS use_partition_pruning = 0;

-- Partition pruning of a mutation.
ALTER TABLE t_name_part DELETE WHERE `if(greater(v, 0), v, 0)` = 9 SETTINGS mutations_sync = 2;
SELECT count() FROM t_name_part WHERE `if(greater(v, 0), v, 0)` = 9 SETTINGS use_partition_pruning = 0;

-- Without the colliding column, the rewritten filter still uses the key.
DROP TABLE IF EXISTS t_no_collision;
CREATE TABLE t_no_collision (v Int32)
ENGINE = MergeTree ORDER BY multiIf(v > 0, v, 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_no_collision SELECT 100 + number FROM numbers(100);
SELECT count() FROM t_no_collision WHERE multiIf(v > 0, v, 0) = 150 SETTINGS force_primary_key = 1;

DROP TABLE t_name_pk;
DROP TABLE t_name_idx;
DROP TABLE t_name_part;
DROP TABLE t_no_collision;
