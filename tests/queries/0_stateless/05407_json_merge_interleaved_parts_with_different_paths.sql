-- A merge inserts into the resulting `JSON` column from the merged parts row by row or by ranges of rows.
-- The parts have different, partially overlapping sets of dynamic paths and paths in shared data.
-- Check that the merged part keeps all values, for horizontal and vertical merges.

DROP TABLE IF EXISTS t_json_merge_source;
DROP TABLE IF EXISTS t_json_merge_rows;
DROP TABLE IF EXISTS t_json_merge_rows_vertical;
DROP TABLE IF EXISTS t_json_merge_ranges;

-- Every part has 20 paths from a different, partially overlapping range and at most 8 of them are dynamic paths.
CREATE TABLE t_json_merge_source ENGINE = Memory AS
SELECT
    intDiv(number, 2000) AS part,
    number,
    concat('{"a":', toString(number % 100), arrayStringConcat(arrayMap(x -> concat(
        ',"k', toString(part * 10 + (cityHash64(number) + x * 3) % 20), '":',
        if(x % 2 = 0, toString(number + x), concat('"s', toString(number), '"'))), range(5)), ''), '}') AS json
FROM numbers(8000);

-- Rows of the parts are interleaved one by one.
CREATE TABLE t_json_merge_rows (id UInt64, number UInt64, json JSON(max_dynamic_paths = 8, a UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_vertical_merge_algorithm = 0, min_parts_to_merge_at_once = 100, index_granularity = 128;

CREATE TABLE t_json_merge_rows_vertical (id UInt64, number UInt64, json JSON(max_dynamic_paths = 8, a UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1, min_parts_to_merge_at_once = 100, index_granularity = 128;

-- Rows of the parts are interleaved in ranges of 50 rows.
CREATE TABLE t_json_merge_ranges (id UInt64, number UInt64, json JSON(max_dynamic_paths = 8, a UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_vertical_merge_algorithm = 0, min_parts_to_merge_at_once = 100, index_granularity = 128;

INSERT INTO t_json_merge_rows SELECT cityHash64(number), number, json FROM t_json_merge_source WHERE part = 0;
INSERT INTO t_json_merge_rows SELECT cityHash64(number), number, json FROM t_json_merge_source WHERE part = 1;
INSERT INTO t_json_merge_rows SELECT cityHash64(number), number, json FROM t_json_merge_source WHERE part = 2;
INSERT INTO t_json_merge_rows SELECT cityHash64(number), number, json FROM t_json_merge_source WHERE part = 3;

INSERT INTO t_json_merge_rows_vertical SELECT cityHash64(number), number, json FROM t_json_merge_source WHERE part = 0;
INSERT INTO t_json_merge_rows_vertical SELECT cityHash64(number), number, json FROM t_json_merge_source WHERE part = 1;
INSERT INTO t_json_merge_rows_vertical SELECT cityHash64(number), number, json FROM t_json_merge_source WHERE part = 2;
INSERT INTO t_json_merge_rows_vertical SELECT cityHash64(number), number, json FROM t_json_merge_source WHERE part = 3;

INSERT INTO t_json_merge_ranges SELECT (intDiv(number % 2000, 50) * 4 + part) * 100000 + number, number, json FROM t_json_merge_source WHERE part = 0;
INSERT INTO t_json_merge_ranges SELECT (intDiv(number % 2000, 50) * 4 + part) * 100000 + number, number, json FROM t_json_merge_source WHERE part = 1;
INSERT INTO t_json_merge_ranges SELECT (intDiv(number % 2000, 50) * 4 + part) * 100000 + number, number, json FROM t_json_merge_source WHERE part = 2;
INSERT INTO t_json_merge_ranges SELECT (intDiv(number % 2000, 50) * 4 + part) * 100000 + number, number, json FROM t_json_merge_source WHERE part = 3;

OPTIMIZE TABLE t_json_merge_rows FINAL;
OPTIMIZE TABLE t_json_merge_rows_vertical FINAL;
OPTIMIZE TABLE t_json_merge_ranges FINAL;

SELECT table, count() FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_json_merge_rows', 't_json_merge_rows_vertical', 't_json_merge_ranges') AND active
GROUP BY table ORDER BY table;

SELECT 'rows', count(), countIf(toString(t.json) = toString(CAST(s.json, 'JSON(a UInt32)')))
FROM t_json_merge_rows AS t INNER JOIN t_json_merge_source AS s USING number;
SELECT 'rows_vertical', count(), countIf(toString(t.json) = toString(CAST(s.json, 'JSON(a UInt32)')))
FROM t_json_merge_rows_vertical AS t INNER JOIN t_json_merge_source AS s USING number;
SELECT 'ranges', count(), countIf(toString(t.json) = toString(CAST(s.json, 'JSON(a UInt32)')))
FROM t_json_merge_ranges AS t INNER JOIN t_json_merge_source AS s USING number;

-- The merged part has the limit of dynamic paths and the rest of the paths in shared data.
SELECT 'rows', length(groupUniqArrayArray(JSONDynamicPaths(json))), countIf(notEmpty(JSONSharedDataPaths(json))) > 0 FROM t_json_merge_rows;
SELECT 'rows_vertical', length(groupUniqArrayArray(JSONDynamicPaths(json))), countIf(notEmpty(JSONSharedDataPaths(json))) > 0 FROM t_json_merge_rows_vertical;
SELECT 'ranges', length(groupUniqArrayArray(JSONDynamicPaths(json))), countIf(notEmpty(JSONSharedDataPaths(json))) > 0 FROM t_json_merge_ranges;

DROP TABLE t_json_merge_source;
DROP TABLE t_json_merge_rows;
DROP TABLE t_json_merge_rows_vertical;
DROP TABLE t_json_merge_ranges;
