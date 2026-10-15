-- Reading subcolumns of a JSON column under FINAL must return correct values when the subcolumn's
-- type and serialization are taken from the part's shared serializations instead of being resolved
-- against the column's serialization in every reader.

DROP TABLE IF EXISTS t_json_subcol_cache_wide;
DROP TABLE IF EXISTS t_json_subcol_cache_compact;

CREATE TABLE t_json_subcol_cache_wide
(
    id UInt64,
    data JSON(a UInt32, b String, c Bool, n JSON(x UInt32))
)
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE t_json_subcol_cache_compact
(
    id UInt64,
    data JSON(a UInt32, b String, c Bool, n JSON(x UInt32))
)
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 100000000;

-- Keep three parts so readers (and their subcolumn resolution) are constructed per part.
SYSTEM STOP MERGES t_json_subcol_cache_wide;
SYSTEM STOP MERGES t_json_subcol_cache_compact;

INSERT INTO t_json_subcol_cache_wide SELECT number, '{"a":' || toString(number) || ',"b":"s' || toString(number) || '","c":' || if(number % 2 = 1, 'true', 'false') || ',"n":{"x":' || toString(number) || '}}' FROM numbers(0, 100);
INSERT INTO t_json_subcol_cache_wide SELECT number, '{"a":' || toString(number) || ',"b":"s' || toString(number) || '","c":' || if(number % 2 = 1, 'true', 'false') || ',"n":{"x":' || toString(number) || '}}' FROM numbers(100, 100);
INSERT INTO t_json_subcol_cache_wide SELECT number, '{"a":' || toString(number) || ',"b":"s' || toString(number) || '","c":' || if(number % 2 = 1, 'true', 'false') || ',"n":{"x":' || toString(number) || '}}' FROM numbers(200, 100);

INSERT INTO t_json_subcol_cache_compact SELECT number, '{"a":' || toString(number) || ',"b":"s' || toString(number) || '","c":' || if(number % 2 = 1, 'true', 'false') || ',"n":{"x":' || toString(number) || '}}' FROM numbers(0, 100);
INSERT INTO t_json_subcol_cache_compact SELECT number, '{"a":' || toString(number) || ',"b":"s' || toString(number) || '","c":' || if(number % 2 = 1, 'true', 'false') || ',"n":{"x":' || toString(number) || '}}' FROM numbers(100, 100);
INSERT INTO t_json_subcol_cache_compact SELECT number, '{"a":' || toString(number) || ',"b":"s' || toString(number) || '","c":' || if(number % 2 = 1, 'true', 'false') || ',"n":{"x":' || toString(number) || '}}' FROM numbers(200, 100);

SELECT 'wide';
SELECT count() FROM t_json_subcol_cache_wide FINAL;
SELECT sum(data.a) FROM t_json_subcol_cache_wide FINAL;
SELECT countIf(data.c) FROM t_json_subcol_cache_wide FINAL;
SELECT count() FROM t_json_subcol_cache_wide FINAL WHERE data.a >= 100 AND data.a < 200;
SELECT count() FROM t_json_subcol_cache_wide FINAL WHERE data.b LIKE 's1%';
SELECT sum(data.n.x) FROM t_json_subcol_cache_wide FINAL;

SELECT 'compact';
SELECT count() FROM t_json_subcol_cache_compact FINAL;
SELECT sum(data.a) FROM t_json_subcol_cache_compact FINAL;
SELECT countIf(data.c) FROM t_json_subcol_cache_compact FINAL;
SELECT count() FROM t_json_subcol_cache_compact FINAL WHERE data.a >= 100 AND data.a < 200;
SELECT count() FROM t_json_subcol_cache_compact FINAL WHERE data.b LIKE 's1%';
SELECT sum(data.n.x) FROM t_json_subcol_cache_compact FINAL;

DROP TABLE t_json_subcol_cache_wide;
DROP TABLE t_json_subcol_cache_compact;
