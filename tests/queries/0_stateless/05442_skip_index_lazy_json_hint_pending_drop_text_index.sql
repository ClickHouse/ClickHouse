-- Tags: no-fasttest, no-random-merge-tree-settings
-- no-fasttest: needs the JSON type.
-- no-random-merge-tree-settings: needs a wide part, so the hint stays unmaterialized and the part records
--   the type the text index was built against.

SET enable_json_lazy_type_hints = 1;

-- A pending DROP COLUMN re-added with the hinted type makes the recorded built-against type match the new
-- metadata, but the reader returns defaults, so neither direct reads from the text index nor the index-only
-- count may use the old postings.
DROP TABLE IF EXISTS t_lazy_drop_text;
CREATE TABLE t_lazy_drop_text (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_lazy_drop_text SELECT number, '{"a":"needle"}' FROM numbers(64);
ALTER TABLE t_lazy_drop_text MODIFY COLUMN j JSON(a String) SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_drop_text ADD INDEX idx j.a TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_drop_text MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SYSTEM STOP MERGES t_lazy_drop_text;
ALTER TABLE t_lazy_drop_text DROP COLUMN j, ADD COLUMN j JSON(a String) SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT '-- rows';
SELECT count(), sum(id) FROM t_lazy_drop_text WHERE hasToken(j.a, 'needle') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count(), sum(id) FROM t_lazy_drop_text WHERE hasToken(j.a, 'needle') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(id) FROM t_lazy_drop_text WHERE hasToken(j.a, 'needle') SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0;

SELECT '-- count';
SELECT count() FROM t_lazy_drop_text WHERE hasToken(j.a, 'needle') SETTINGS query_plan_optimize_count_from_text_index = 1;
SELECT count() FROM t_lazy_drop_text WHERE hasToken(j.a, 'needle') SETTINGS query_plan_optimize_count_from_text_index = 0, use_skip_indexes = 0;

DROP TABLE t_lazy_drop_text;
