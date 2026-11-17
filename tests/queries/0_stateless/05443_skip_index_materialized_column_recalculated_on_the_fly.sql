-- Tags: no-fasttest, no-random-merge-tree-settings
-- no-fasttest: needs the JSON type.
-- no-random-merge-tree-settings: needs wide parts, so the lazy hint stays unmaterialized and the part records
--   the type the index was built against.

SET enable_json_lazy_type_hints = 1;

-- An on-the-fly UPDATE recalculates the MATERIALIZED columns computed from the updated column, so a skip index
-- over such a column is stale even though the UPDATE does not assign it.

SELECT '-- plain MATERIALIZED column';
DROP TABLE IF EXISTS t_mat_plain;
CREATE TABLE t_mat_plain (id UInt32, v UInt64, c UInt64 MATERIALIZED v * 3, INDEX idx c TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_plain (id, v) SELECT number, number FROM numbers(64);
SYSTEM STOP MERGES t_mat_plain;
ALTER TABLE t_mat_plain UPDATE v = 333333 WHERE id = 0 SETTINGS mutations_sync = 0;
SELECT count() FROM t_mat_plain WHERE c = 999999 SETTINGS apply_mutations_on_fly = 1;
SELECT count() FROM t_mat_plain WHERE c = 999999 SETTINGS apply_mutations_on_fly = 1, use_skip_indexes = 0;
DROP TABLE t_mat_plain;

SELECT '-- lazily hinted JSON MATERIALIZED column, minmax index';
DROP TABLE IF EXISTS t_mat_json;
CREATE TABLE t_mat_json (id UInt32, v String, j JSON MATERIALIZED v::JSON)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_json SELECT number, toJSONString(map('a', toString(number * 3))) FROM numbers(64);
ALTER TABLE t_mat_json MODIFY COLUMN j JSON(a UInt64) MATERIALIZED CAST(v, 'JSON(a UInt64)') SETTINGS alter_sync = 2;
ALTER TABLE t_mat_json ADD INDEX idx JSONExtractUInt(toJSONString(j), 'a') TYPE minmax GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_mat_json MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_mat_json;
ALTER TABLE t_mat_json UPDATE v = '{"a":999999}' WHERE id = 0 SETTINGS mutations_sync = 0;
SELECT count() FROM t_mat_json WHERE JSONExtractUInt(toJSONString(j), 'a') = 999999 SETTINGS apply_mutations_on_fly = 1;
SELECT count() FROM t_mat_json WHERE JSONExtractUInt(toJSONString(j), 'a') = 999999
SETTINGS apply_mutations_on_fly = 1, use_skip_indexes = 0;
DROP TABLE t_mat_json;

SELECT '-- lazily hinted JSON MATERIALIZED column, text index';
DROP TABLE IF EXISTS t_mat_text;
CREATE TABLE t_mat_text (id UInt32, v String, j JSON MATERIALIZED v::JSON)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_text SELECT number, '{"a":"hay"}' FROM numbers(64);
ALTER TABLE t_mat_text MODIFY COLUMN j JSON(a String) MATERIALIZED CAST(v, 'JSON(a String)') SETTINGS alter_sync = 2;
ALTER TABLE t_mat_text ADD INDEX idx JSONExtractString(toJSONString(j), 'a') TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
SETTINGS alter_sync = 2;
ALTER TABLE t_mat_text MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_mat_text;
ALTER TABLE t_mat_text UPDATE v = '{"a":"needle"}' WHERE id = 0 SETTINGS mutations_sync = 0;
SELECT count() FROM t_mat_text WHERE hasToken(JSONExtractString(toJSONString(j), 'a'), 'needle')
SETTINGS apply_mutations_on_fly = 1, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM t_mat_text WHERE hasToken(JSONExtractString(toJSONString(j), 'a'), 'needle')
SETTINGS apply_mutations_on_fly = 1, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_mat_text WHERE hasToken(JSONExtractString(toJSONString(j), 'a'), 'needle')
SETTINGS apply_mutations_on_fly = 1, use_skip_indexes = 0;
DROP TABLE t_mat_text;
