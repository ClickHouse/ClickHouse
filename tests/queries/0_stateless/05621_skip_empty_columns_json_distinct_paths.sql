-- The distinct-paths subcolumn of a `JSON` column holds the paths in the first row of a block and nothing in the other rows.
-- A `JSON` column omitted from the part (all values are the default) must be filled the same way.

DROP TABLE IF EXISTS t_json_omitted_wide;
DROP TABLE IF EXISTS t_json_omitted_compact;

CREATE TABLE t_json_omitted_wide (id UInt64, j JSON(a UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_json_omitted_wide (id) SELECT number FROM numbers(5);

SELECT 'wide part columns', column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_json_omitted_wide' AND active ORDER BY column;
SELECT id, j.__special_subcolumn_name_for_distinct_paths_calculation FROM t_json_omitted_wide ORDER BY id;
SELECT distinctJSONPaths(j), distinctJSONPathsAndTypes(j) FROM t_json_omitted_wide;
SELECT id, j FROM t_json_omitted_wide ORDER BY id LIMIT 2;

CREATE TABLE t_json_omitted_compact (id UInt64, j JSON(a UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_json_omitted_compact (id) SELECT number FROM numbers(5);

SELECT 'compact part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_json_omitted_compact' AND active;
SELECT id, j.__special_subcolumn_name_for_distinct_paths_calculation FROM t_json_omitted_compact ORDER BY id;
SELECT distinctJSONPaths(j) FROM t_json_omitted_compact;

DROP TABLE t_json_omitted_wide;
DROP TABLE t_json_omitted_compact;
