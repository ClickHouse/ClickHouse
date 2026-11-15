-- Tags: no-fasttest, no-random-merge-tree-settings
-- no-fasttest: needs the JSON type.
-- no-random-merge-tree-settings: needs a wide part, so the hint stays unmaterialized and the part records
--   the type the index granules were built against.

SET enable_json_lazy_type_hints = 1;

-- A pending DROP COLUMN re-added with the hinted type makes the recorded built-against type match the new
-- metadata, but the reader returns the replacement column's defaults, so the old granules must not prune.
DROP TABLE IF EXISTS t_lazy_drop;
CREATE TABLE t_lazy_drop (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_lazy_drop SELECT number, toJSONString(map('a', toString(number * 3))) FROM numbers(64);
ALTER TABLE t_lazy_drop MODIFY COLUMN j JSON(a UInt64) SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_drop ADD INDEX idx j.a TYPE minmax GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_lazy_drop MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SYSTEM STOP MERGES t_lazy_drop;
ALTER TABLE t_lazy_drop DROP COLUMN j, ADD COLUMN j JSON(a UInt64) SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT '-- pruning';
SELECT count() FROM t_lazy_drop WHERE j.a = 0;
SELECT count() FROM t_lazy_drop WHERE j.a = 0 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_lazy_drop WHERE j.a = 0 SETTINGS use_skip_indexes_on_data_read = 1;

SELECT '-- top-k';
INSERT INTO t_lazy_drop VALUES (64, '{"a":10}');
SELECT j.a FROM t_lazy_drop ORDER BY j.a DESC LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100000;
SELECT j.a FROM t_lazy_drop ORDER BY j.a DESC LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0;

DROP TABLE t_lazy_drop;
