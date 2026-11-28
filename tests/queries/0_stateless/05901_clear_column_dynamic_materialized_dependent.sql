-- A CLEAR COLUMN recomputes the MATERIALIZED columns that depend on the cleared column. On a Wide part the recomputed
-- value of a JSON or Dynamic column can have fewer streams than the stored one, and the new part must not keep them.

SET mutations_sync = 2, alter_sync = 2;

DROP TABLE IF EXISTS t_json;
CREATE TABLE t_json (k UInt32, b UInt32, a JSON MATERIALIZED if(b = 0, '{}', '{"p":1}'))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_json (k, b) VALUES (1, 41);
ALTER TABLE t_json CLEAR COLUMN b;
SELECT 'json', k, b, a.p FROM t_json;
CHECK TABLE t_json SETTINGS check_query_single_value_result = 1;
SELECT 'json-count', count() FROM t_json;
DROP TABLE t_json;

DROP TABLE IF EXISTS t_dynamic;
CREATE TABLE t_dynamic (k UInt32, b String, a Dynamic MATERIALIZED if(b = '', CAST(NULL, 'Dynamic'), CAST(41::Int64, 'Dynamic')))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_dynamic (k, b) VALUES (1, 'x');
ALTER TABLE t_dynamic CLEAR COLUMN b;
SELECT 'dynamic', k, b, a, dynamicType(a) FROM t_dynamic;
CHECK TABLE t_dynamic SETTINGS check_query_single_value_result = 1;
SELECT 'dynamic-count', count() FROM t_dynamic;
DROP TABLE t_dynamic;

-- Only the part of the cleared partition is rewritten.
DROP TABLE IF EXISTS t_partition;
CREATE TABLE t_partition (p UInt8, k UInt32, b UInt32, a JSON MATERIALIZED if(b = 0, '{}', '{"p":1}'))
ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_partition (p, k, b) VALUES (1, 1, 41), (2, 2, 41);
ALTER TABLE t_partition CLEAR COLUMN b IN PARTITION 1;
SELECT 'partition', p, k, b, a.p FROM t_partition ORDER BY p;
CHECK TABLE t_partition SETTINGS check_query_single_value_result = 1;
SELECT 'partition-count', count() FROM t_partition;
DROP TABLE t_partition;
