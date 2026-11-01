SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_structure_hint_alias;
CREATE TABLE t_structure_hint_alias (id UInt32, v UInt32) ENGINE = Memory;

INSERT INTO FUNCTION file(currentDatabase() || '_structure_hint_alias.jsonl', 'JSONEachRow', 'id UInt32, v UInt32') SELECT number, number * 10 FROM numbers(1, 3) SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO t_structure_hint_alias SELECT f.id, f.v FROM file(currentDatabase() || '_structure_hint_alias.jsonl') AS f;
INSERT INTO t_structure_hint_alias SELECT f.id, f.v FROM file(currentDatabase() || '_structure_hint_alias.jsonl') AS f WHERE f.id > 1;

SELECT * FROM t_structure_hint_alias ORDER BY ALL;

DROP TABLE t_structure_hint_alias;
