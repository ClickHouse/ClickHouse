-- Tags: no-fasttest, no-random-merge-tree-settings
-- no-fasttest: needs the JSON type.
-- no-random-merge-tree-settings: needs a wide part, so the hint stays unmaterialized and the part records
--   the type the index was built against.

SET enable_json_lazy_type_hints = 1;

-- A pending rename that swaps two column names keeps a column named `j`, so ALTER validation accepts it,
-- but `j` now reads another column. The index granules (and the recorded type) describe the old `j`, so the
-- index must not be used even though the recorded type still matches the new `j.a`.
DROP TABLE IF EXISTS t_rename_swap;
CREATE TABLE t_rename_swap (id UInt32, j JSON, other JSON(a UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_rename_swap SELECT number, toJSONString(map('a', toString(number * 3))), '{"a":999999}' FROM numbers(64);
ALTER TABLE t_rename_swap MODIFY COLUMN j JSON(a UInt64) SETTINGS alter_sync = 2;
ALTER TABLE t_rename_swap ADD INDEX idx j.a TYPE minmax GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_rename_swap MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SYSTEM STOP MERGES t_rename_swap;
ALTER TABLE t_rename_swap RENAME COLUMN j TO old_j, RENAME COLUMN other TO j SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT '-- pruning';
SELECT count() FROM t_rename_swap WHERE j.a = 999999;
SELECT count() FROM t_rename_swap WHERE j.a = 999999 SETTINGS use_skip_indexes = 0;

SELECT '-- top-k';
SELECT j.a FROM t_rename_swap ORDER BY j.a DESC LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100000;
SELECT j.a FROM t_rename_swap ORDER BY j.a DESC LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0;

DROP TABLE t_rename_swap;
