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

INSERT INTO FUNCTION file(currentDatabase() || '_structure_hint_alias_dotted.jsonl', 'JSONEachRow', '`f.id` UInt32, id UInt32') SELECT 7, 1 SETTINGS engine_file_truncate_on_insert = 1;

SELECT f.id, id FROM file(currentDatabase() || '_structure_hint_alias_dotted.jsonl') AS f;
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT f.id, id FROM file(currentDatabase() || '_structure_hint_alias_dotted.jsonl') AS f;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

SELECT f.id, id FROM file(currentDatabase() || '_structure_hint_alias_dotted.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT f.id, id FROM file(currentDatabase() || '_structure_hint_alias_dotted.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

INSERT INTO FUNCTION file(currentDatabase() || '_structure_hint_alias_subcolumn.jsonl', 'JSONEachRow', 'f Tuple(id UInt32), id UInt32') SELECT tuple(7), 1 SETTINGS engine_file_truncate_on_insert = 1;

SELECT f.id, id FROM file(currentDatabase() || '_structure_hint_alias_subcolumn.jsonl') AS f;
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT f.id, id FROM file(currentDatabase() || '_structure_hint_alias_subcolumn.jsonl') AS f;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

SELECT f.id, id FROM file(currentDatabase() || '_structure_hint_alias_subcolumn.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT f.id, id FROM file(currentDatabase() || '_structure_hint_alias_subcolumn.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

INSERT INTO FUNCTION file(currentDatabase() || '_structure_hint_alias_ambiguous.jsonl', 'LineAsString', 'line String') SELECT arrayJoin(['{"f.id":7,"id":1,"unused":{"a":1}}', '{"f.id":8,"id":2,"unused":{"a":{"b":1}}}']) SETTINGS engine_file_truncate_on_insert = 1;

DESCRIBE file(currentDatabase() || '_structure_hint_alias_ambiguous.jsonl'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT f.id, id FROM file(currentDatabase() || '_structure_hint_alias_ambiguous.jsonl') AS f;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

INSERT INTO FUNCTION file(currentDatabase() || '_structure_hint_alias_nested.jsonl', 'JSONEachRow', '`f.t.a` UInt32, t Tuple(a UInt32)') SELECT 7, tuple(1) SETTINGS engine_file_truncate_on_insert = 1;

SELECT f.t.a, f.`t.a` FROM file(currentDatabase() || '_structure_hint_alias_nested.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT f.t.a, f.`t.a` FROM file(currentDatabase() || '_structure_hint_alias_nested.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

INSERT INTO FUNCTION file(currentDatabase() || '_structure_hint_alias_quoted.jsonl', 'JSONEachRow', 't Tuple(a UInt32), v UInt32') SELECT tuple(7), 10 SETTINGS engine_file_truncate_on_insert = 1;

SELECT f.`t.a`, v FROM file(currentDatabase() || '_structure_hint_alias_quoted.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0;
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT f.`t.a`, v FROM file(currentDatabase() || '_structure_hint_alias_quoted.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

SELECT f.`t.a`, v FROM file(currentDatabase() || '_structure_hint_alias_quoted.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT f.`t.a`, v FROM file(currentDatabase() || '_structure_hint_alias_quoted.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

SELECT `t.a`, v FROM file(currentDatabase() || '_structure_hint_alias_quoted.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0;
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT `t.a`, v FROM file(currentDatabase() || '_structure_hint_alias_quoted.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

SELECT `t.a`, v FROM file(currentDatabase() || '_structure_hint_alias_quoted.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
TRUNCATE TABLE t_structure_hint_alias;
INSERT INTO t_structure_hint_alias SELECT `t.a`, v FROM file(currentDatabase() || '_structure_hint_alias_quoted.jsonl') AS f SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT * FROM t_structure_hint_alias ORDER BY ALL;

DROP TABLE t_structure_hint_alias;
