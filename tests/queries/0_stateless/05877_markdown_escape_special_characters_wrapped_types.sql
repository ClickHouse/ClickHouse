SET output_format_markdown_escape_special_characters = 1;

SELECT 'a|b*c' AS s, toNullable('a|b*c') AS n, toLowCardinality('a|b*c') AS l, toLowCardinality(toNullable('a|b*c')) AS ln, CAST(NULL AS Nullable(String)) AS nn FORMAT Markdown;
SELECT CAST('a|b*c' AS Variant(String, UInt64)) AS v, 'a|b*c'::Dynamic AS d, CAST('a|b*c' AS Dynamic(max_types = 0)) AS d_shared, toNullable(toFixedString('a|b', 3)) AS nf, toNullable(CAST(1 AS Enum8('a|b' = 1))) AS ne, toNullable(true) AS nb FORMAT Markdown;

DROP TABLE IF EXISTS t_markdown_sparse;
CREATE TABLE t_markdown_sparse (id UInt32, s String) ENGINE = MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0;
INSERT INTO t_markdown_sparse SELECT number, if(number IN (2, 7), 'a|b*c', '') FROM numbers(20);
SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_markdown_sparse' AND active ORDER BY column;
SELECT s FROM t_markdown_sparse WHERE id IN (1, 2, 7) ORDER BY id FORMAT Markdown SETTINGS optimize_read_in_order = 1, allow_special_serialization_kinds_in_output_formats = 1;
SELECT s FROM t_markdown_sparse WHERE id IN (1, 2, 7) ORDER BY id FORMAT Markdown SETTINGS optimize_read_in_order = 1, allow_special_serialization_kinds_in_output_formats = 0;
DROP TABLE t_markdown_sparse;

DROP TABLE IF EXISTS t_markdown_replicated;
CREATE TABLE t_markdown_replicated (s String, arr Array(UInt8)) ENGINE = Memory;
INSERT INTO t_markdown_replicated VALUES ('a|b*c', [1, 2]);
SELECT s, a FROM t_markdown_replicated ARRAY JOIN arr AS a FORMAT Markdown SETTINGS enable_lazy_columns_replication = 1, allow_special_serialization_kinds_in_output_formats = 1;
SELECT s, a FROM t_markdown_replicated ARRAY JOIN arr AS a FORMAT Markdown SETTINGS enable_lazy_columns_replication = 1, allow_special_serialization_kinds_in_output_formats = 0;
DROP TABLE t_markdown_replicated;

SET output_format_markdown_escape_special_characters = 0;
SELECT toNullable('a|b*c') AS n, toLowCardinality('a|b*c') AS l, CAST('a|b*c' AS Variant(String, UInt64)) AS v, 'a|b*c'::Dynamic AS d FORMAT Markdown;
