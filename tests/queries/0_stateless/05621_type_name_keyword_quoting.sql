-- Data type names are persisted: `Variant` orders its alternatives by their names and names their streams in
-- parts after them. Names inside a type name keep the spelling of earlier releases, even the ones that are quoted
-- when formatting queries, like `not`, `as`, `interval` and `strict`.

SET flatten_nested = 0;
SET print_pretty_type_names = 0;

SELECT toTypeName(CAST(tuple(1, 2, 3, 4), 'Tuple(not UInt8, as UInt8, interval UInt8, strict UInt8)'));
SELECT toTypeName(CAST(tuple(1, 2), 'Tuple(`table` UInt8, `top` UInt8)'));
SELECT toTypeName(CAST(tuple(1, 2), 'Tuple(interval UInt8, `table` UInt8)')) SETTINGS print_pretty_type_names = 1;
SELECT toTypeName(CAST('{}', 'JSON(interval UInt32, SKIP not)'));

SELECT toTypeName(v), toTypeName(variantType(v))
FROM (SELECT CAST(CAST(tuple(1), 'Tuple(interval UInt32)'), 'Variant(Tuple(a String), Tuple(interval UInt32))') AS v);

DROP TABLE IF EXISTS t_type_name_keyword_quoting;

CREATE TABLE t_type_name_keyword_quoting (n Nested(not UInt8), v Variant(String, Tuple(interval UInt64)), d Dynamic)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_type_name_keyword_quoting SELECT [tuple(1)], CAST(tuple(42), 'Tuple(interval UInt64)'), CAST(tuple(7), 'Tuple(strict UInt64)');

SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_type_name_keyword_quoting' ORDER BY name;

SELECT column, arrayFilter(s -> s LIKE '%interval', substreams)
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_type_name_keyword_quoting' AND active AND column = 'v';

SELECT n, v, v.`Tuple(interval UInt64)`, d, dynamicType(d) FROM t_type_name_keyword_quoting;

DROP TABLE t_type_name_keyword_quoting;
