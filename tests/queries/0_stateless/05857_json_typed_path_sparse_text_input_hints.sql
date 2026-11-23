-- A text-format INSERT must not parse into sparse `JSON` typed paths taken from the table's serialization hints, #133480.

DROP TABLE IF EXISTS t_json_sparse_parse_hints;
DROP TABLE IF EXISTS t_json_sparse_parse_hints_tuple;
DROP TABLE IF EXISTS t_json_sparse_parse_hints_typed_tuple;

-- Typed path `x` of a plain `JSON` column, inserted from `JSONEachRow` and `TSV`.
CREATE TABLE t_json_sparse_parse_hints (id UInt64, j JSON(x String, max_dynamic_paths = 0))
ENGINE = MergeTree ORDER BY id
SETTINGS serialization_info_version = 'with_subcolumns', ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_json_sparse_parse_hints SELECT number, if(number = 0, '{"x":"a"}', '{}') FROM numbers(100);

SELECT countIf(subcolumns.serializations[indexOf(subcolumns.names, 'x')] = 'Sparse') > 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_sparse_parse_hints' AND column = 'j' AND active;

INSERT INTO FUNCTION file(currentDatabase() || '_json_sparse_parse_hints.jsonl', JSONEachRow)
SELECT 100 + number AS id, if(number = 0, '{"x":"b"}', '{}')::JSON(x String, max_dynamic_paths = 0) AS j FROM numbers(2)
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO t_json_sparse_parse_hints
SELECT * FROM file(currentDatabase() || '_json_sparse_parse_hints.jsonl', JSONEachRow, 'id UInt64, j JSON(x String, max_dynamic_paths = 0)')
SETTINGS enable_parsing_to_custom_serialization = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_json_sparse_parse_hints.tsv', TSV)
SELECT 200 + number AS id, if(number = 0, '{"x":"c"}', '{}')::JSON(x String, max_dynamic_paths = 0) AS j FROM numbers(2)
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO t_json_sparse_parse_hints
SELECT * FROM file(currentDatabase() || '_json_sparse_parse_hints.tsv', TSV, 'id UInt64, j JSON(x String, max_dynamic_paths = 0)')
SETTINGS enable_parsing_to_custom_serialization = 1;

SELECT j.x, count() FROM t_json_sparse_parse_hints GROUP BY j.x ORDER BY j.x;

-- `JSON` inside a top-level `Tuple`.
CREATE TABLE t_json_sparse_parse_hints_tuple (id UInt64, tj Tuple(s String, j JSON(x String, max_dynamic_paths = 0)))
ENGINE = MergeTree ORDER BY id
SETTINGS serialization_info_version = 'with_subcolumns', ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_json_sparse_parse_hints_tuple SELECT number, tuple(toString(number), if(number = 0, '{"x":"a"}', '{}')) FROM numbers(100);

SELECT countIf(subcolumns.serializations[indexOf(subcolumns.names, 'j.x')] = 'Sparse') > 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_sparse_parse_hints_tuple' AND column = 'tj' AND active;

INSERT INTO FUNCTION file(currentDatabase() || '_json_sparse_parse_hints_tuple.jsonl', JSONEachRow)
SELECT 100 + number AS id, tuple(toString(number), if(number = 0, '{"x":"b"}', '{}'))::Tuple(s String, j JSON(x String, max_dynamic_paths = 0)) AS tj FROM numbers(2)
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO t_json_sparse_parse_hints_tuple
SELECT * FROM file(currentDatabase() || '_json_sparse_parse_hints_tuple.jsonl', JSONEachRow, 'id UInt64, tj Tuple(s String, j JSON(x String, max_dynamic_paths = 0))')
SETTINGS enable_parsing_to_custom_serialization = 1;

SELECT tj.j.x, count() FROM t_json_sparse_parse_hints_tuple GROUP BY tj.j.x ORDER BY tj.j.x;

-- Typed path of `Tuple` type, whose elements are stored Sparse.
CREATE TABLE t_json_sparse_parse_hints_typed_tuple (id UInt64, j JSON(t Tuple(a String, b UInt64), max_dynamic_paths = 0))
ENGINE = MergeTree ORDER BY id
SETTINGS serialization_info_version = 'with_subcolumns', ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_json_sparse_parse_hints_typed_tuple SELECT number, if(number = 0, '{"t":{"a":"x","b":1}}', '{}') FROM numbers(100);

SELECT countIf(subcolumns.serializations[indexOf(subcolumns.names, 't.b')] = 'Sparse') > 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_sparse_parse_hints_typed_tuple' AND column = 'j' AND active;

INSERT INTO FUNCTION file(currentDatabase() || '_json_sparse_parse_hints_typed_tuple.jsonl', JSONEachRow)
SELECT 100 + number AS id, if(number = 0, '{"t":{"a":"y","b":2}}', '{}')::JSON(t Tuple(a String, b UInt64), max_dynamic_paths = 0) AS j FROM numbers(2)
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO t_json_sparse_parse_hints_typed_tuple
SELECT * FROM file(currentDatabase() || '_json_sparse_parse_hints_typed_tuple.jsonl', JSONEachRow, 'id UInt64, j JSON(t Tuple(a String, b UInt64), max_dynamic_paths = 0)')
SETTINGS enable_parsing_to_custom_serialization = 1;

SELECT j.t, count() FROM t_json_sparse_parse_hints_typed_tuple GROUP BY j.t ORDER BY j.t;

DROP TABLE t_json_sparse_parse_hints;
DROP TABLE t_json_sparse_parse_hints_tuple;
DROP TABLE t_json_sparse_parse_hints_typed_tuple;
