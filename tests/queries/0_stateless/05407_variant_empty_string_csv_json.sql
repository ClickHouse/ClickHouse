-- https://github.com/ClickHouse/ClickHouse/issues/126607
SELECT 'CSV', variantType(v), v FROM format(CSV, 'v Variant(String, Int64)', '""\n"x"\n5\n"5"\n');
SELECT 'CSV reversed order', variantType(v), v FROM format(CSV, 'v Variant(Int64, String)', '""\n');
SELECT 'CSV UInt8', variantType(v), v FROM format(CSV, 'v Variant(String, UInt8)', '""\n');
SELECT 'CSV UInt256', variantType(v), v FROM format(CSV, 'v Variant(String, UInt256)', '""\n');
SELECT 'CSV single quotes', variantType(v), v FROM format(CSV, 'v Variant(String, Int64)', '\'\'\n') SETTINGS format_csv_allow_single_quotes = 1;
SELECT 'JSONEachRow', variantType(v), v FROM format(JSONEachRow, 'v Variant(String, Int64)', '{"v": ""}\n{"v": "x"}\n{"v": 5}\n{"v": "5"}\n');
SELECT 'JSONEachRow UInt8', variantType(v), v FROM format(JSONEachRow, 'v Variant(String, UInt8)', '{"v": ""}\n');
SELECT 'JSONEachRow Array', v, variantType(v[1]) FROM format(JSONEachRow, 'v Array(Variant(String, Int64))', '{"v": ["", 1]}\n');
SELECT 'CSV Tuple', variantType(v), v FROM format(CSV, 'v Variant(String, Tuple(Int64))', '""\n');
SELECT 'CSV Float64 imprecise', variantType(v), v FROM format(CSV, 'v Variant(String, Float64)', '""\n') SETTINGS precise_float_parsing = 0;
SELECT 'JSONEachRow Float64 imprecise', variantType(v), v FROM format(JSONEachRow, 'v Variant(String, Float64)', '{"v": ""}\n') SETTINGS precise_float_parsing = 0;
SELECT 'CustomSeparated CSV rule', variantType(v), v FROM format(CustomSeparated, 'v Variant(String, Int64)', '""\n') SETTINGS format_custom_escaping_rule = 'CSV';
SELECT 'TSV', variantType(v), v FROM format(TSV, 'v Variant(String, Int64)', '\nx\n5\n');
-- Without a String alternative, "" is not a number either.
SELECT * FROM format(CSV, 'v Variant(Int64, Float64)', '""\n') SETTINGS precise_float_parsing = 1; -- { serverError INCORRECT_DATA }
-- Round trip through ClickHouse's own output.
DROP TABLE IF EXISTS t_126607;
CREATE TABLE t_126607 (id Int64, v Variant(String, Int64)) ENGINE = Memory;
INSERT INTO t_126607 VALUES (1, ''::String), (2, 'x'::String), (3, 5::Int64);
SET engine_file_truncate_on_insert = 1; -- the test may run several times in a row
INSERT INTO FUNCTION file(currentDatabase() || '_126607.csv', CSV) SELECT * FROM t_126607;
INSERT INTO FUNCTION file(currentDatabase() || '_126607.jsonl', JSONEachRow) SELECT * FROM t_126607;
SELECT 'from CSV', id, variantType(v), v FROM file(currentDatabase() || '_126607.csv', CSV, 'id Int64, v Variant(String, Int64)') ORDER BY id;
SELECT 'from JSONEachRow', id, variantType(v), v FROM file(currentDatabase() || '_126607.jsonl', JSONEachRow, 'id Int64, v Variant(String, Int64)') ORDER BY id;
DROP TABLE t_126607;
