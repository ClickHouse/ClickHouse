-- A JSON object without some typed paths of the JSON type it is inserted into gets the defaults of their types,
-- not the zeroes of their columns (a zero is not always a value of the type, as for an `Enum`).

SELECT 'conversion of an object, inserted into Array and Variant';
SELECT * FROM values('a Array(JSON(e Enum8(\'minus\' = -1, \'zero\' = 0)))', ([CAST('{"x" : 1}' AS JSON)]));
SELECT * FROM values('a Array(JSON(e Enum8(\'ok\' = 1)))', ([CAST('{"x" : 1}' AS JSON)]));
SELECT * FROM values('v Variant(JSON(e Enum8(\'minus\' = -1, \'zero\' = 0)), UInt64)', (CAST('{"x" : 1}' AS JSON)));
SELECT * FROM values('v Variant(JSON(e Enum8(\'ok\' = 1)), UInt64)', (CAST('{"x" : 1}' AS JSON)));

SELECT 'with the source, the text is created from the converted object';
SELECT a, arrayMap(x -> getSubcolumn(x, '__source'), a) FROM values('a Array(JSON(with_source=1, e Enum8(\'minus\' = -1, \'zero\' = 0)))', ([CAST('{"x" : 1}' AS JSON)]));
SELECT v, getSubcolumn(variantElement(v, 'JSON(with_source=1, e Enum8(\'minus\' = -1, \'zero\' = 0))'), '__source') FROM values('v Variant(JSON(with_source=1, e Enum8(\'minus\' = -1, \'zero\' = 0)), UInt64)', (CAST('{"x" : 1}' AS JSON)));

SELECT 'values of typed paths are converted to their types';
SELECT * FROM values('a Array(JSON(e Enum8(\'ok\' = 1)))', ([CAST('{"e" : "ok"}' AS JSON)]));
SELECT * FROM values('a Array(JSON(n UInt8))', ([CAST('{"n" : "42"}' AS JSON)]));
SELECT * FROM values('v Variant(JSON(n UInt8), UInt64)', (CAST('{"n" : "42"}' AS JSON)));
SELECT * FROM values('a Array(JSON(n UInt8))', ([CAST('{"n" : null}' AS JSON(n Nullable(UInt8)))]));
SELECT * FROM values('a Array(JSON(n UInt8))', ([CAST('{"n" : 300}' AS JSON)])); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT 'values of typed paths are converted from the types of the source paths';
SELECT j.e, j.__source FROM values('j JSON(with_source=1, e String)', CAST('{"e" : "ok"}' AS JSON(e Enum8('ok' = 1))));
SELECT j.d FROM values('j JSON(d DateTime(\'UTC\'))', CAST('{"d" : "2020-01-02"}' AS JSON(d Date)));

SELECT 'the elements of nested types are converted from their source types';
SELECT * FROM values('a Array(JSON(e String))', ([CAST('{"e" : "ok"}' AS JSON(e Enum8('ok' = 1)))]));
SELECT * FROM values('t Tuple(String)', (tuple(CAST('ok' AS Enum8('ok' = 1)))));
SELECT * FROM values('m Map(String, String)', (map('k', CAST('ok' AS Enum8('ok' = 1)))));

SELECT 'skipped paths are dropped';
SELECT * FROM values('a Array(JSON(SKIP s, SKIP REGEXP \'^r\'))', ([CAST('{"s" : {"x" : 1}, "sx" : 2, "r1" : 3, "x" : 4}' AS JSON)]));
SELECT * FROM values('a Array(JSON(SKIP REGEXP \'^r\'))', ([CAST('{"r1" : 3, "x" : 4}' AS JSON)])) SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0;

SELECT 'the path of the source is not allowed in a type with the source';
SELECT * FROM values('a Array(JSON(with_source=1))', ([CAST('{"__source" : 1, "x" : 2}' AS JSON)])); -- { serverError INCORRECT_DATA }
SELECT * FROM values('v Variant(JSON(with_source=1), UInt64)', (CAST('{"__source" : 1, "x" : 2}' AS JSON))); -- { serverError TYPE_MISMATCH }
-- `__source-x` sorts between `__source` and `__source.y`.
SELECT * FROM values('v Variant(JSON(with_source=1), UInt64)', (CAST('{"__source" : {"y" : 1}, "__source-x" : 2}' AS JSON))); -- { serverError TYPE_MISMATCH }

SELECT 'default of the type';
SELECT defaultValueOfTypeName('JSON(e Enum8(\'ok\' = 1), n UInt32)');

SELECT 'a column the part marks as missing';
DROP TABLE IF EXISTS t_json_missing_column;
CREATE TABLE t_json_missing_column (id UInt64, j JSON(n UInt32, s String)) ENGINE = MergeTree ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_json_missing_column (id) VALUES (1);
SELECT j FROM t_json_missing_column;
DROP TABLE t_json_missing_column;

SELECT 'objects in a Variant are converted to the type';
SELECT JSONAllPaths(variantElement(v, 'JSON(SKIP secret)')) FROM values('v Variant(JSON(SKIP secret), UInt64)', (CAST('{"secret" : 1, "x" : 2}' AS JSON)));
SELECT v FROM values('v Variant(Array(JSON(SKIP secret)), UInt64)', ([CAST('{"secret" : 1, "x" : 2}' AS JSON)]));
SELECT v, variantType(v) FROM values('v Variant(JSON, String)', ('{"a" : 1}'));

SELECT 'NULL and invalid values of typed paths';
SELECT * FROM values('j JSON(n UInt8)', (CAST('{"n" : null}' AS JSON(n Nullable(UInt8)))));
SELECT * FROM values('j JSON(n UInt8)', (CAST('{"n" : null}' AS JSON(n Nullable(UInt8))))) SETTINGS input_format_null_as_default = 0; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('j JSON(n UInt8)', (CAST('{"n" : null}' AS JSON(n Nullable(UInt8))))) SETTINGS input_format_null_as_default = 0, type_json_skip_invalid_typed_paths = 1;
SELECT * FROM values('j JSON(n UInt8)', (CAST('{"n" : "abc"}' AS JSON(n String)))); -- { serverError TYPE_MISMATCH }
SELECT * FROM values('j JSON(n UInt8)', (CAST('{"n" : "abc"}' AS JSON(n String)))) SETTINGS type_json_skip_invalid_typed_paths = 1;

SELECT 'nested timestamps out of the range of DateTime';
SELECT * FROM values('t Tuple(DateTime(\'UTC\'))', (tuple(toDate32('1900-01-01')))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('a Array(DateTime(\'UTC\'))', ([toDate32('1900-01-01')])); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('m Map(String, DateTime(\'UTC\'))', (map('k', toDate32('1900-01-01')))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('j JSON(d DateTime(\'UTC\'))', (CAST('{"d" : "1900-01-01"}' AS JSON(d Date32)))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('t Tuple(DateTime(\'UTC\'))', (tuple(toDate32('2020-01-02'))));
