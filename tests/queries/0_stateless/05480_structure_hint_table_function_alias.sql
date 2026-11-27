SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_structure_hint_alias;
CREATE TABLE t_structure_hint_alias (id UInt32, v UInt32) ENGINE = Memory;

INSERT INTO FUNCTION file(currentDatabase() || '_structure_hint_alias.jsonl', 'JSONEachRow', 'id UInt32, v UInt32') SELECT number, number * 10 FROM numbers(1, 3) SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO t_structure_hint_alias SELECT f.id, f.v FROM file(currentDatabase() || '_structure_hint_alias.jsonl') AS f;
INSERT INTO t_structure_hint_alias SELECT f.id, f.v FROM file(currentDatabase() || '_structure_hint_alias.jsonl') AS f WHERE f.id > 1;

SELECT * FROM t_structure_hint_alias ORDER BY ALL;

TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT id, g.v FROM file(currentDatabase() || '_structure_hint_alias.jsonl') AS f JOIN (SELECT 1 AS id2, 99 AS v) AS g ON id = g.id2;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

INSERT INTO FUNCTION file(currentDatabase() || '_structure_hint_alias_tuple.jsonl', 'JSONEachRow', 'v UInt32, t Tuple(a UInt32)') SELECT number * 10, tuple(number * 100) FROM numbers(1, 2) SETTINGS engine_file_truncate_on_insert = 1;

TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT f.t.a, f.v FROM file(currentDatabase() || '_structure_hint_alias_tuple.jsonl') AS f;
INSERT INTO t_structure_hint_alias SELECT t.a, v FROM file(currentDatabase() || '_structure_hint_alias_tuple.jsonl');
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

-- A quoted name with a dot can be a subcolumn too.
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT f.`t.a`, f.v FROM file(currentDatabase() || '_structure_hint_alias_tuple.jsonl') AS f;
INSERT INTO t_structure_hint_alias SELECT `t.a`, v FROM file(currentDatabase() || '_structure_hint_alias_tuple.jsonl');
INSERT INTO t_structure_hint_alias SELECT f.`t.a`, f.v FROM file(currentDatabase() || '_structure_hint_alias_tuple.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT id, g.v FROM file(currentDatabase() || '_structure_hint_alias.jsonl') AS f JOIN (SELECT 1 AS id2, 99 AS v) AS g ON id = g.id2 SETTINGS use_structure_from_insertion_table_in_table_functions = 1;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

-- The hint is the only way to read a file whose structure cannot be inferred, so the alias must be stripped without looking at the file.
INSERT INTO FUNCTION file(currentDatabase() || '_structure_hint_alias_nulls.jsonl', 'LineAsString', 'line String') SELECT arrayJoin(['{"id":null,"v":null}', '{"id":1,"v":10}', '{"id":2,"v":20}']) SETTINGS engine_file_truncate_on_insert = 1;

SET input_format_max_rows_to_read_for_schema_inference = 1, input_format_json_infer_incomplete_types_as_strings = 0;
DESCRIBE file(currentDatabase() || '_structure_hint_alias_nulls.jsonl'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT f.id, f.v FROM file(currentDatabase() || '_structure_hint_alias_nulls.jsonl') AS f;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;
SET input_format_max_rows_to_read_for_schema_inference = DEFAULT, input_format_json_infer_incomplete_types_as_strings = DEFAULT;

-- With the forced hint a quoted name with a dot is hinted, it is the way to read a physical column like a flattened `Nested` one.
DROP TABLE IF EXISTS t_structure_hint_alias_nested;
CREATE TABLE t_structure_hint_alias_nested (n Nested(a UInt32, b UInt32)) ENGINE = Memory;
INSERT INTO FUNCTION file(currentDatabase() || '_structure_hint_alias_nested.tsv', 'TSV', 'n Nested(a UInt32, b UInt32)') SELECT [1, 2], [3, 4] SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO t_structure_hint_alias_nested SELECT `n.a`, `n.b` FROM file(currentDatabase() || '_structure_hint_alias_nested.tsv', 'TSV') SETTINGS use_structure_from_insertion_table_in_table_functions = 1;
INSERT INTO t_structure_hint_alias_nested SELECT f.`n.a`, f.`n.b` FROM file(currentDatabase() || '_structure_hint_alias_nested.tsv', 'TSV') AS f SETTINGS use_structure_from_insertion_table_in_table_functions = 1;
SELECT * FROM t_structure_hint_alias_nested;
DROP TABLE t_structure_hint_alias_nested;

DROP TABLE IF EXISTS t_structure_hint_alias_dotted;
CREATE TABLE t_structure_hint_alias_dotted (`a.b` UInt32) ENGINE = Memory;
INSERT INTO FUNCTION file(currentDatabase() || '_structure_hint_alias_dotted_nulls.jsonl', 'LineAsString', 'line String') SELECT arrayJoin(['{"a.b":null}', '{"a.b":42}']) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO t_structure_hint_alias_dotted SELECT `a.b` FROM file(currentDatabase() || '_structure_hint_alias_dotted_nulls.jsonl') SETTINGS use_structure_from_insertion_table_in_table_functions = 1, input_format_max_rows_to_read_for_schema_inference = 1, input_format_json_infer_incomplete_types_as_strings = 0;
SELECT * FROM t_structure_hint_alias_dotted ORDER BY ALL;
DROP TABLE t_structure_hint_alias_dotted;

-- generateRandom and null take their structure from the hint, so a name with a dot is a column there, like in input.
DROP TABLE IF EXISTS t_structure_hint_alias_generated;
CREATE TABLE t_structure_hint_alias_generated (n Nested(a UInt32, b UInt32)) ENGINE = Memory;
INSERT INTO t_structure_hint_alias_generated SELECT n.a, n.b FROM generateRandom(1, 10, 0) LIMIT 1;
INSERT INTO t_structure_hint_alias_generated SELECT g.n.a, g.n.b FROM generateRandom(1, 10, 0) AS g LIMIT 1;
INSERT INTO t_structure_hint_alias_generated SELECT `n.a`, `n.b` FROM generateRandom(1, 10, 0) LIMIT 1;
INSERT INTO t_structure_hint_alias_generated SELECT n.a, n.b FROM null();
INSERT INTO t_structure_hint_alias_generated SELECT `n.a`, `n.b` FROM null();
SELECT * FROM t_structure_hint_alias_generated;
DROP TABLE t_structure_hint_alias_generated;

-- A column of a CTE or of a temporary table is not a column of generateRandom.
SET query_plan_join_swap_table = 'false';
DROP TABLE IF EXISTS t_structure_hint_alias_cte;
CREATE TABLE t_structure_hint_alias_cte (id UInt32, v UInt32) ENGINE = Memory;
INSERT INTO t_structure_hint_alias_cte WITH g AS (SELECT 99 AS v) SELECT id, g.v FROM generateRandom(1, 10, 0) AS r CROSS JOIN g LIMIT 1;
INSERT INTO t_structure_hint_alias_cte WITH g AS (SELECT 98 AS v UNION ALL SELECT 98) SELECT id, g.v FROM generateRandom(1, 10, 0) AS r CROSS JOIN g LIMIT 1;
SET enable_materialized_cte = 1;
INSERT INTO t_structure_hint_alias_cte WITH g AS MATERIALIZED (SELECT 97 AS v) SELECT id, g.v FROM generateRandom(1, 10, 0) AS r CROSS JOIN g LIMIT 1;
SET enable_materialized_cte = DEFAULT;
CREATE TEMPORARY TABLE t_structure_hint_alias_temporary (v UInt32);
INSERT INTO t_structure_hint_alias_temporary VALUES (96);
INSERT INTO t_structure_hint_alias_cte SELECT id, t_structure_hint_alias_temporary.v FROM generateRandom(1, 10, 0) AS r CROSS JOIN t_structure_hint_alias_temporary LIMIT 1;
SELECT v FROM t_structure_hint_alias_cte ORDER BY v DESC;
SET query_plan_join_swap_table = DEFAULT;
DROP TABLE t_structure_hint_alias_cte;

-- Choosing the hint column names must not run the schema inference of the source.
TRUNCATE TABLE t_structure_hint_alias;
SET log_comment = '05480_structure_hint_alias_no_inference';
INSERT INTO t_structure_hint_alias SELECT f.id, f.v FROM file(currentDatabase() || '_structure_hint_alias.jsonl') AS f;
SET log_comment = DEFAULT;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['SchemaInferenceCacheMisses'] + ProfileEvents['SchemaInferenceCacheHits'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = '05480_structure_hint_alias_no_inference';

DROP TABLE t_structure_hint_alias;
