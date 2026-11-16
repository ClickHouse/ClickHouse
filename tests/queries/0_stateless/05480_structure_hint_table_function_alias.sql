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

-- Choosing the hint column names must not run the schema inference of the source.
TRUNCATE TABLE t_structure_hint_alias;
SET log_comment = '05480_structure_hint_alias_no_inference';
INSERT INTO t_structure_hint_alias SELECT f.id, f.v FROM file(currentDatabase() || '_structure_hint_alias.jsonl') AS f;
SET log_comment = DEFAULT;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['SchemaInferenceCacheMisses'] + ProfileEvents['SchemaInferenceCacheHits'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05480_structure_hint_alias_no_inference';

DROP TABLE t_structure_hint_alias;
