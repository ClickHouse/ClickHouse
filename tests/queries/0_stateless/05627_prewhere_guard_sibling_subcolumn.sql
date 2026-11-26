-- A condition on one subcolumn keeps guarding a throwing condition on a sibling subcolumn of the same column
-- when WHERE is moved to PREWHERE, although the throwing condition reads less and would be ordered first.
-- https://github.com/ClickHouse/ClickHouse/issues/130086

SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1;
SET optimize_functions_to_subcolumns = 1, use_statistics = 0, move_all_conditions_to_prewhere = 1;

DROP TABLE IF EXISTS t_guard_tuple;
DROP TABLE IF EXISTS t_guard_json;
DROP TABLE IF EXISTS t_guard_map;

CREATE TABLE t_guard_tuple (id UInt64, t Tuple(safe String, val String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_guard_tuple SELECT number, if(number % 3 = 0, ('', 'n/a'), (hex(MD5(toString(number))), toString(number % 100))) FROM numbers(10000);

CREATE TABLE t_guard_json (id UInt64, j JSON(safe String, val String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_guard_json SELECT number, if(number % 3 = 0, '{"safe": "", "val": "n/a"}', toJSONString(map('safe', hex(MD5(toString(number))), 'val', toString(number % 100)))) FROM numbers(10000);

CREATE TABLE t_guard_map (id UInt64, tags Map(String, String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant', max_buckets_in_map = 2, map_buckets_min_avg_size = 0;
INSERT INTO t_guard_map SELECT number, if(number % 3 = 0, map('safe', '', 'val', 'n/a'), map('safe', hex(MD5(toString(number))), 'val', toString(number % 100))) FROM numbers(10000);

SELECT count() FROM t_guard_tuple WHERE t.safe != '' AND toUInt64(t.val) > 50;
SELECT count() FROM t_guard_json WHERE j.safe != '' AND toUInt64(j.val) > 50;
SELECT count() FROM t_guard_map WHERE tags['safe'] != '' AND toUInt64(tags['val']) > 50;

DROP TABLE t_guard_tuple;
DROP TABLE t_guard_json;
DROP TABLE t_guard_map;
